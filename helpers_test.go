package totallytics

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

var testKey = "tt_" + strings.Repeat("ab", 24)

type received struct {
	header  http.Header
	body    []byte
	payload payload
}

type ingestServer struct {
	*httptest.Server
	mu       sync.Mutex
	received []received
}

// newIngest fakes the ingest API, answering the nth request with respond(n, p).
func newIngest(t *testing.T, respond func(n int, p payload) int) *ingestServer {
	t.Helper()
	s := &ingestServer{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read ingest body: %v", err)
		}
		var p payload
		if err := json.Unmarshal(body, &p); err != nil {
			t.Errorf("decode ingest body: %v", err)
		}

		s.mu.Lock()
		s.received = append(s.received, received{header: r.Header.Clone(), body: body, payload: p})
		n := len(s.received)
		s.mu.Unlock()

		status := http.StatusAccepted
		if respond != nil {
			status = respond(n, p)
		}
		w.WriteHeader(status)
		if status == http.StatusAccepted {
			fmt.Fprintf(w, `{"accepted":{"metrics":%d,"errors":%d},"rejected":0}`, len(p.Metrics), len(p.Errors))
		}
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *ingestServer) requests() []received {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.received)
}

func (s *ingestServer) metrics() []metricRow {
	var rows []metricRow
	for _, r := range s.requests() {
		rows = append(rows, r.payload.Metrics...)
	}
	return rows
}

func (s *ingestServer) samples() []errorRow {
	var rows []errorRow
	for _, r := range s.requests() {
		rows = append(rows, r.payload.Errors...)
	}
	return rows
}

func newTestClient(t *testing.T, ingest *ingestServer, opts Options) *Client {
	t.Helper()
	opts.APIKey = testKey
	opts.Endpoint = ingest.URL
	c := newClient(opts, func(c *Client) {
		c.transport.backoffBase = time.Millisecond
	})
	t.Cleanup(func() { _ = c.Shutdown(context.Background()) })
	return c
}

func newTestTransport(t *testing.T, endpoint string, logs *logRecorder) *transport {
	t.Helper()
	tr := newTransport(endpoint, testKey, logger{slog.New(logs)})
	tr.backoffBase = time.Millisecond
	t.Cleanup(tr.abort)
	return tr
}

func shutdown(t *testing.T, c *Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

func testBatch(t *testing.T, metrics, samples int) *batch {
	t.Helper()
	rows := make([]metricRow, metrics)
	for i := range rows {
		rows[i] = metricRow{Minute: 1790000000, Method: "GET", Route: fmt.Sprintf("/r/%d", i), Status: 200, Count: 1, Histogram: map[int]int64{0: 1}}
	}
	errs := make([]errorRow, samples)
	for i := range errs {
		errs[i] = errorRow{TS: 1790000000000, Method: "GET", Route: "/r/0", Path: "/r/0", Status: 500}
	}
	return newBatch("totallytics-go/test", rows, errs)
}

func do(h http.Handler, method, target string, header ...string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, target, nil)
	for i := 0; i+1 < len(header); i += 2 {
		r.Header.Set(header[i], header[i+1])
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func byRoute(rows []metricRow) map[string]metricRow {
	routes := make(map[string]metricRow, len(rows))
	for _, row := range rows {
		merged := routes[row.Route]
		merged.Route, merged.Method, merged.Status, merged.Consumer, merged.UserAgent = row.Route, row.Method, row.Status, row.Consumer, row.UserAgent
		merged.Count += row.Count
		routes[row.Route] = merged
	}
	return routes
}

func histogramTotal(row metricRow) int64 {
	var total int64
	for _, n := range row.Histogram {
		total += n
	}
	return total
}

var ok = http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})

type logRecorder struct {
	mu    sync.Mutex
	lines []string
}

func (h *logRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (h *logRecorder) Handle(_ context.Context, r slog.Record) error {
	line := r.Level.String() + " " + r.Message
	r.Attrs(func(a slog.Attr) bool {
		line += " " + a.String()
		return true
	})
	h.mu.Lock()
	h.lines = append(h.lines, line)
	h.mu.Unlock()
	return nil
}

func (h *logRecorder) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *logRecorder) WithGroup(string) slog.Handler { return h }

func (h *logRecorder) count(substr string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, line := range h.lines {
		if strings.Contains(line, substr) {
			n++
		}
	}
	return n
}
