// Package openaicompat 在任何「OpenAI 兼容」的 chat-completions 端点上、以
// SSE 流式方式实现 adapter.LLM 接口（DeepSeek、Qwen、Moonshot、OpenAI…）。
// 它把自己注册为 "openai-compat"；导入它（cmd/server 里的空白导入）即可让它
// 在配置中可选。
//
// SSE 客户端是基于 net/http 手写的：流式协议解析器恰恰是本项目的题中之义，
// 而且这样保持依赖面为零。
package openaicompat

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"voicestream/internal/adapter"
	"voicestream/internal/config"
)

func init() {
	adapter.RegisterLLM("openai-compat", func(cfg config.Config) (adapter.LLM, error) {
		c := cfg.Adapters.CloudLLM
		if c.BaseURL == "" || c.Model == "" {
			return nil, fmt.Errorf("openaicompat: adapters.cloud_llm.base_url and .model must be set")
		}
		key := os.Getenv(c.APIKeyEnv)
		if key == "" {
			return nil, fmt.Errorf("openaicompat: API key env %s is empty", c.APIKeyEnv)
		}
		l := New(c.BaseURL, c.Model, key)
		l.systemPrompt = c.SystemPrompt // 前置 system 消息，约束为纯口语文本（见 config）
		return l, nil
	})
}

// LLM 从一个 OpenAI 兼容端点流式获取 chat completions。
type LLM struct {
	baseURL      string
	model        string
	apiKey       string
	systemPrompt string // 非空则作为首条 system 消息前置到每次请求
	client       *http.Client
}

// New 返回针对给定端点的适配器。baseURL 是 API 根（如
// "https://api.deepseek.com/v1"）；路径 /chat/completions 按 OpenAI 线路
// 约定追加在后面。
func New(baseURL, model, apiKey string) *LLM {
	return &LLM{
		baseURL: strings.TrimRight(baseURL, "/"),
		model:   model,
		apiKey:  apiKey,
		// 不设整体超时：流是长生命周期的，取消来自 ctx（打断）。拨号超时
		// 另行限定。
		client: &http.Client{Transport: &http.Transport{
			ResponseHeaderTimeout: 30 * time.Second,
		}},
	}
}

// 请求/响应的线路结构（只取我们用到的子集）。
type chatRequest struct {
	Model    string        `json:"model"`
	Stream   bool          `json:"stream"`
	Messages []chatMessage `json:"messages"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatChunk struct {
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
}

// Stream 实现 adapter.LLM。它把每个 SSE delta 作为一个 Token 产出，在服务器
// 发来 [DONE]、流结束、或 ctx 被取消时返回——取消会中止底层 HTTP 请求，于是
// 连接（以及服务商那边的生成）及时停止，这正是打断所需要的。
func (l *LLM) Stream(ctx context.Context, turn adapter.Turn, out chan<- adapter.Token) error {
	msgs := make([]chatMessage, 0, len(turn.History)+2)
	if l.systemPrompt != "" {
		// 每次请求都新鲜前置 system 消息——而非种进 History（那会被流水线的
		// maxHistory 截断在几轮后丢掉）。
		msgs = append(msgs, chatMessage{Role: "system", Content: l.systemPrompt})
	}
	for _, m := range turn.History {
		msgs = append(msgs, chatMessage{Role: m.Role, Content: m.Text})
	}
	msgs = append(msgs, chatMessage{Role: "user", Content: turn.Prompt})

	body, err := json.Marshal(chatRequest{Model: l.model, Stream: true, Messages: msgs})
	if err != nil {
		return fmt.Errorf("openaicompat: marshal request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		l.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("openaicompat: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Authorization", "Bearer "+l.apiKey)

	resp, err := l.client.Do(req)
	if err != nil {
		return fmt.Errorf("openaicompat: request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("openaicompat: %s: %s", resp.Status, strings.TrimSpace(string(detail)))
	}

	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		data, ok := strings.CutPrefix(sc.Text(), "data:")
		if !ok {
			continue // 空的保活行、": 注释"、"event:" 字段
		}
		data = strings.TrimSpace(data)
		if data == "[DONE]" {
			return nil
		}
		var chunk chatChunk
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			return fmt.Errorf("openaicompat: bad SSE chunk: %w", err)
		}
		if len(chunk.Choices) == 0 {
			continue
		}
		if text := chunk.Choices[0].Delta.Content; text != "" {
			select {
			case out <- adapter.Token{Text: text}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	// 读 body 的错误在此浮现——包括流中途因 ctx 取消导致的中止，我们把它
	// 如实报告为「取消」。
	if err := sc.Err(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("openaicompat: read stream: %w", err)
	}
	return nil
}
