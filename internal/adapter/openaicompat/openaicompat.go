// Package openaicompat implements the adapter.LLM interface over any
// OpenAI-compatible chat-completions endpoint with SSE streaming (DeepSeek,
// Qwen, Moonshot, OpenAI, ...). It registers itself as "openai-compat";
// importing it (blank import in cmd/server) makes it selectable from config.
//
// The SSE client is hand-rolled on net/http: a streaming protocol parser is
// exactly this project's business, and it keeps the dependency surface zero.
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
		return New(c.BaseURL, c.Model, key), nil
	})
}

// LLM streams chat completions from an OpenAI-compatible endpoint.
type LLM struct {
	baseURL string
	model   string
	apiKey  string
	client  *http.Client
}

// New returns an adapter for the given endpoint. baseURL is the API root
// (e.g. "https://api.deepseek.com/v1"); the path /chat/completions is
// appended per the OpenAI wire convention.
func New(baseURL, model, apiKey string) *LLM {
	return &LLM{
		baseURL: strings.TrimRight(baseURL, "/"),
		model:   model,
		apiKey:  apiKey,
		// No overall timeout: streams are long-lived and cancellation comes
		// from ctx (barge-in). The dial is bounded separately.
		client: &http.Client{Transport: &http.Transport{
			ResponseHeaderTimeout: 30 * time.Second,
		}},
	}
}

// Request/response wire shapes (the subset we use).
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

// Stream implements adapter.LLM. It emits each SSE delta as one Token and
// returns when the server sends [DONE], the stream ends, or ctx is cancelled
// — cancellation aborts the underlying HTTP request, so the connection (and
// the provider's generation) stops promptly, which is what barge-in needs.
func (l *LLM) Stream(ctx context.Context, turn adapter.Turn, out chan<- adapter.Token) error {
	msgs := make([]chatMessage, 0, len(turn.History)+1)
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
			continue // blank keep-alive lines, ": comments", "event:" fields
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
	// Body read errors surface here — including the abort caused by ctx
	// cancellation mid-stream, which we report as the cancellation it is.
	if err := sc.Err(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("openaicompat: read stream: %w", err)
	}
	return nil
}
