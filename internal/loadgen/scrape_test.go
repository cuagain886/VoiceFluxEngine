package loadgen

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

const expositionFixture = `# HELP voicestream_first_response_seconds Utterance end to first downlink audio frame.
# TYPE voicestream_first_response_seconds histogram
voicestream_first_response_seconds_bucket{le="0.005"} 0
voicestream_first_response_seconds_bucket{le="0.01"} 2
voicestream_first_response_seconds_bucket{le="+Inf"} 4
voicestream_first_response_seconds_sum 1.23
voicestream_first_response_seconds_count 4
# HELP voicestream_turns_completed_total Turns that ran to natural completion.
# TYPE voicestream_turns_completed_total counter
voicestream_turns_completed_total 4
voicestream_goroutines 12
`

func TestScrapeParsesScalarsAndHistograms(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(expositionFixture))
	}))
	defer ts.Close()

	snap, err := scrape(context.Background(), ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	if got := snap.scalars["voicestream_turns_completed_total"]; got != 4 {
		t.Fatalf("turns scalar = %v, want 4", got)
	}
	if got := snap.scalars["voicestream_goroutines"]; got != 12 {
		t.Fatalf("goroutines = %v, want 12", got)
	}
	h := snap.hists["voicestream_first_response_seconds"]
	if h == nil || h[0.01] != 2 || h[inf] != 4 {
		t.Fatalf("histogram parse = %v", h)
	}

	// Delta against an empty baseline, then quantile.
	zero := &snapshot{scalars: map[string]float64{}, hists: map[string]map[float64]float64{}}
	bounds, counts := histDelta(zero, snap, "voicestream_first_response_seconds")
	if len(bounds) != 3 || counts[1] != 2 || counts[2] != 2 {
		t.Fatalf("histDelta bounds=%v counts=%v", bounds, counts)
	}
	if q := histQuantile(bounds, counts, 0.5); q != 0.01 {
		t.Fatalf("q50 = %v, want 0.01", q)
	}
	if d := scalarDelta(zero, snap, "voicestream_turns_completed_total"); d != 4 {
		t.Fatalf("scalarDelta = %v, want 4", d)
	}
}

func TestScrapeRejectsNon200(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()
	if _, err := scrape(context.Background(), ts.URL); err == nil {
		t.Fatal("expected error on 404")
	}
}
