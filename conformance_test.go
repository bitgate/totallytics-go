package totallytics

import (
	"encoding/json"
	"errors"
	"math"
	"os"
	"slices"
	"strconv"
	"testing"
)

// testdata/conformance.json holds what the JavaScript SDK sends for the same
// requests; testdata/gen-conformance.mjs regenerates it.
type conformanceFixture struct {
	SDK     string               `json:"sdk"`
	Buckets [][2]json.RawMessage `json:"buckets"`
	Cases   []struct {
		Name         string         `json:"name"`
		MaxBatchRows int            `json:"max_batch_rows"`
		Entries      []fixtureEntry `json:"entries"`
		Batches      []any          `json:"batches"`
	} `json:"cases"`
}

type fixtureEntry struct {
	Method     string  `json:"method"`
	Path       string  `json:"path"`
	Route      string  `json:"route"`
	Status     int     `json:"status"`
	DurationMs float64 `json:"durationMs"`
	StartedAt  int64   `json:"startedAt"`
	UserAgent  string  `json:"userAgent"`
	Consumer   string  `json:"consumer"`
	Error      string  `json:"error"`
}

func loadConformance(t *testing.T) conformanceFixture {
	t.Helper()
	raw, err := os.ReadFile("testdata/conformance.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture conformanceFixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatalf("decode conformance.json: %v", err)
	}
	return fixture
}

func TestConformanceBuckets(t *testing.T) {
	fixture := loadConformance(t)
	mismatches := 0
	for _, pair := range fixture.Buckets {
		ms, err := parseJSNumber(pair[0])
		if err != nil {
			t.Fatal(err)
		}
		var want int
		if err := json.Unmarshal(pair[1], &want); err != nil {
			t.Fatal(err)
		}
		if got := bucket(ms); got != want {
			mismatches++
			if mismatches <= 10 {
				t.Errorf("bucket(%v) = %d, %s says %d", ms, got, fixture.SDK, want)
			}
		}
	}
	if mismatches > 0 {
		t.Errorf("%d of %d buckets differ", mismatches, len(fixture.Buckets))
	}
}

func TestConformanceBatches(t *testing.T) {
	fixture := loadConformance(t)
	for _, tc := range fixture.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			ingest := newIngest(t, nil)
			c := newTestClient(t, ingest, Options{MaxBatchRows: tc.MaxBatchRows})
			for _, e := range tc.Entries {
				var err error
				if e.Error != "" {
					err = errors.New(e.Error)
				}
				c.record(entry{
					method:     e.Method,
					path:       e.Path,
					route:      e.Route,
					status:     e.Status,
					durationMs: e.DurationMs,
					startedAt:  e.StartedAt,
					userAgent:  e.UserAgent,
					consumer:   e.Consumer,
					err:        err,
				})
			}
			shutdown(t, c)

			var sent []any
			for _, r := range ingest.requests() {
				var batch struct {
					Metrics any `json:"metrics"`
					Errors  any `json:"errors"`
				}
				if err := json.Unmarshal(r.body, &batch); err != nil {
					t.Fatal(err)
				}
				sent = append(sent, map[string]any{"metrics": batch.Metrics, "errors": batch.Errors})
			}

			got, want := canonical(t, sent), canonical(t, tc.Batches)
			if len(got) != len(want) {
				t.Fatalf("sent %d batches, %s sent %d", len(got), fixture.SDK, len(want))
			}
			for i := range want {
				if got[i] != want[i] {
					t.Errorf("batch %d differs\n go: %.2000s\n js: %.2000s", i, got[i], want[i])
				}
			}
		})
	}
}

func parseJSNumber(raw json.RawMessage) (float64, error) {
	var text string
	if json.Unmarshal(raw, &text) == nil {
		switch text {
		case "NaN":
			return math.NaN(), nil
		case "Infinity":
			return math.Inf(1), nil
		case "-Infinity":
			return math.Inf(-1), nil
		}
	}
	return strconv.ParseFloat(string(raw), 64)
}

// canonical re-encodes batches with sorted keys, in a stable order, since
// deliveries may arrive in any order.
func canonical(t *testing.T, batches []any) []string {
	t.Helper()
	encoded := make([]string, len(batches))
	for i, b := range batches {
		raw, err := json.Marshal(b)
		if err != nil {
			t.Fatal(err)
		}
		encoded[i] = string(raw)
	}
	slices.Sort(encoded)
	return encoded
}
