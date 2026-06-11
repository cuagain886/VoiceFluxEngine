package openaicompat

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"voicestream/internal/adapter"
)

// sseHandler streams the given deltas in OpenAI chat-completion chunk shape,
// interleaved with the noise real providers emit (comments, blank lines).
func sseHandler(t *testing.T, deltas []string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Errorf("Authorization = %q", got)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		fmt.Fprint(w, ": keep-alive comment\n\n")
		for _, d := range deltas {
			fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":%q}}]}\n\n", d)
			fl.Flush()
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
		fl.Flush()
	}
}

func collect(t *testing.T, llm *LLM, turn adapter.Turn) []string {
	t.Helper()
	out := make(chan adapter.Token, 64)
	if err := llm.Stream(context.Background(), turn, out); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	close(out)
	var got []string
	for tok := range out {
		got = append(got, tok.Text)
	}
	return got
}

func TestStreamParsesSSE(t *testing.T) {
	deltas := []string{"Hel", "lo", " wor", "ld"}
	ts := httptest.NewServer(sseHandler(t, deltas))
	defer ts.Close()

	got := collect(t, New(ts.URL, "test-model", "test-key"), adapter.Turn{Prompt: "hi"})
	if len(got) != len(deltas) {
		t.Fatalf("tokens = %d, want %d (must be incremental, not one blob)", len(got), len(deltas))
	}
	if joined := strings.Join(got, ""); joined != "Hello world" {
		t.Fatalf("concatenation = %q", joined)
	}
}

func TestStreamSendsHistory(t *testing.T) {
	var sawBody string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 4096)
		n, _ := r.Body.Read(buf)
		sawBody = string(buf[:n])
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer ts.Close()

	turn := adapter.Turn{
		Prompt:  "and now?",
		History: []adapter.Message{{Role: "user", Text: "before"}, {Role: "assistant", Text: "reply"}},
	}
	collect(t, New(ts.URL, "test-model", "test-key"), turn)

	for _, want := range []string{`"before"`, `"reply"`, `"and now?"`, `"stream":true`} {
		if !strings.Contains(sawBody, want) {
			t.Fatalf("request body missing %s: %s", want, sawBody)
		}
	}
}

func TestStreamErrorStatus(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"invalid api key"}`, http.StatusUnauthorized)
	}))
	defer ts.Close()

	err := New(ts.URL, "m", "test-key").Stream(context.Background(), adapter.Turn{}, make(chan adapter.Token, 1))
	if err == nil || !strings.Contains(err.Error(), "invalid api key") {
		t.Fatalf("err = %v, want surfaced provider detail", err)
	}
}

// TestStreamCancelMidStream proves barge-in semantics against a server that
// would happily stream forever: cancel must abort the request promptly.
func TestStreamCancelMidStream(t *testing.T) {
	release := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"first\"}}]}\n\n")
		fl.Flush()
		<-release // hold the stream open, never finishing
	}))
	defer ts.Close()
	defer close(release)

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan adapter.Token, 1)
	errc := make(chan error, 1)
	go func() { errc <- New(ts.URL, "m", "test-key").Stream(ctx, adapter.Turn{}, out) }()

	<-out // first token arrived; the model is "mid-sentence"
	cancel()

	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Stream did not return after cancel")
	}
}

// TestLiveProvider talks to a real endpoint and is skipped unless a key is
// present (VOICESTREAM_LLM_API_KEY; optionally VOICESTREAM_LLM_BASE_URL and
// VOICESTREAM_LLM_MODEL). This is the 4.4 acceptance against real token
// timing: run it manually or in a credentialed CI lane.
func TestLiveProvider(t *testing.T) {
	key := os.Getenv("VOICESTREAM_LLM_API_KEY")
	if key == "" {
		t.Skip("VOICESTREAM_LLM_API_KEY not set; skipping live provider test")
	}
	base := os.Getenv("VOICESTREAM_LLM_BASE_URL")
	if base == "" {
		base = "https://api.deepseek.com/v1"
	}
	model := os.Getenv("VOICESTREAM_LLM_MODEL")
	if model == "" {
		model = "deepseek-chat"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	llm := New(base, model, key)
	out := make(chan adapter.Token, 256)
	errc := make(chan error, 1)
	go func() { errc <- llm.Stream(ctx, adapter.Turn{Prompt: "用一句话介绍你自己。"}, out) }()

	tokens := 0
	for {
		select {
		case tok := <-out:
			tokens++
			t.Logf("token %d: %q", tokens, tok.Text)
		case err := <-errc:
			if err != nil {
				t.Fatalf("Stream: %v", err)
			}
			if tokens < 2 {
				t.Fatalf("got %d tokens; want an incremental stream", tokens)
			}
			return
		case <-ctx.Done():
			t.Fatal("live stream timed out")
		}
	}
}
