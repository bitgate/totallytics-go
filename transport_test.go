package totallytics

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRetriesResendIdenticalBytes(t *testing.T) {
	ingest := newIngest(t, func(n int, _ payload) int {
		if n < 3 {
			return http.StatusServiceUnavailable
		}
		return http.StatusAccepted
	})
	tr := newTestTransport(t, ingest.URL, &logRecorder{})
	b := testBatch(t, 3, 1)
	<-tr.deliver(b).done

	requests := ingest.requests()
	if len(requests) != 3 {
		t.Fatalf("got %d requests, want 3", len(requests))
	}
	for i, r := range requests {
		if !bytes.Equal(r.body, b.body) || r.payload.BatchID != b.id {
			t.Errorf("attempt %d sent different bytes or batch id %q", i+1, r.payload.BatchID)
		}
	}
}

func TestRetryableStatusesUseThreeAttempts(t *testing.T) {
	for _, status := range []int{408, 429, 500, 502, 503} {
		ingest := newIngest(t, func(int, payload) int { return status })
		tr := newTestTransport(t, ingest.URL, &logRecorder{})
		<-tr.deliver(testBatch(t, 1, 0)).done
		if got := len(ingest.requests()); got != maxAttempts {
			t.Errorf("HTTP %d: got %d requests, want %d", status, got, maxAttempts)
		}
	}
}

func TestNetworkErrorsAreRetried(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= 2 {
			conn, _, err := http.NewResponseController(w).Hijack()
			if err == nil {
				conn.Close()
			}
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	tr := newTestTransport(t, server.URL, &logRecorder{})
	<-tr.deliver(testBatch(t, 1, 0)).done
	if got := calls.Load(); got != 3 {
		t.Errorf("got %d requests, want 3", got)
	}
}

func TestAttemptsTimeOut(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
	defer server.Close()

	tr := newTestTransport(t, server.URL, &logRecorder{})
	tr.timeout = 50 * time.Millisecond
	start := time.Now()
	<-tr.deliver(testBatch(t, 1, 0)).done
	if got := calls.Load(); got != maxAttempts {
		t.Errorf("got %d requests, want %d", got, maxAttempts)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("delivery took %v", elapsed)
	}
}

func TestClientErrorsAreDropped(t *testing.T) {
	for _, status := range []int{400, 403, 404, 422} {
		ingest := newIngest(t, func(int, payload) int { return status })
		tr := newTestTransport(t, ingest.URL, &logRecorder{})
		<-tr.deliver(testBatch(t, 1, 0)).done
		if got := len(ingest.requests()); got != 1 {
			t.Errorf("HTTP %d: got %d requests, want 1", status, got)
		}
	}
}

func TestUnauthorizedWarnsOncePerProcess(t *testing.T) {
	unauthorizedWarned.Store(false)
	t.Cleanup(func() { unauthorizedWarned.Store(false) })

	ingest := newIngest(t, func(int, payload) int { return http.StatusUnauthorized })
	logs := &logRecorder{}
	for range 2 {
		tr := newTestTransport(t, ingest.URL, logs)
		<-tr.deliver(testBatch(t, 1, 0)).done
		<-tr.deliver(testBatch(t, 1, 0)).done
	}

	if got := len(ingest.requests()); got != 4 {
		t.Errorf("got %d requests, want 4 without retries", got)
	}
	if got := logs.count("WARN totallytics: ingest rejected the API key (401)"); got != 1 {
		t.Errorf("warned %d times, want once", got)
	}
}

func TestTooLargeBatchesAreSplit(t *testing.T) {
	ingest := newIngest(t, func(_ int, p payload) int {
		if len(p.Metrics) > 2 {
			return http.StatusRequestEntityTooLarge
		}
		return http.StatusAccepted
	})
	tr := newTestTransport(t, ingest.URL, &logRecorder{})
	original := testBatch(t, 10, 3)
	<-tr.deliver(original).done

	ids := map[string]bool{}
	metrics, samples := 0, 0
	for _, r := range ingest.requests() {
		if ids[r.payload.BatchID] {
			t.Errorf("batch id %s sent for different contents", r.payload.BatchID)
		}
		ids[r.payload.BatchID] = true
		if len(r.payload.Metrics) <= 2 {
			metrics += len(r.payload.Metrics)
			samples += len(r.payload.Errors)
			if r.payload.BatchID == original.id {
				t.Error("a split half reused the original batch id")
			}
		}
	}
	if metrics != 10 || samples != 3 {
		t.Errorf("accepted %d metrics and %d errors, want 10 and 3", metrics, samples)
	}
	if len(tr.pending) != 0 || len(tr.running) != 0 {
		t.Errorf("%d deliveries still pending, %d running", len(tr.pending), len(tr.running))
	}
}

func TestUnsplittableBatchIsDropped(t *testing.T) {
	ingest := newIngest(t, func(int, payload) int { return http.StatusRequestEntityTooLarge })
	logs := &logRecorder{}
	tr := newTestTransport(t, ingest.URL, logs)
	<-tr.deliver(testBatch(t, 1, 0)).done
	if got := len(ingest.requests()); got != 1 {
		t.Errorf("got %d requests, want 1", got)
	}
	if logs.count("too large and cannot be split") != 1 {
		t.Error("missing log for the unsplittable batch")
	}
}

func TestPendingCapDropsOldest(t *testing.T) {
	release := make(chan struct{})
	var arrived sync.WaitGroup
	arrived.Add(maxPending + 1)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= maxPending+1 {
			arrived.Done()
			<-release
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	logs := &logRecorder{}
	tr := newTestTransport(t, server.URL, logs)
	deliveries := make([]*delivery, 0, maxPending+1)
	for range maxPending + 1 {
		deliveries = append(deliveries, tr.deliver(testBatch(t, 1, 0)))
	}
	arrived.Wait()
	close(release)
	for _, d := range deliveries {
		<-d.done
	}

	if got, want := calls.Load(), int32(1+maxPending*maxAttempts); got != want {
		t.Errorf("got %d requests, want %d", got, want)
	}
	if !deliveries[0].dropped.Load() {
		t.Error("oldest delivery was not dropped")
	}
	if logs.count("more than 20 batches pending") != 1 {
		t.Error("missing log for the dropped batch")
	}
}

func TestHalve(t *testing.T) {
	original := testBatch(t, 3, 3)
	first, second, ok := halve(original)
	if !ok {
		t.Fatal("could not halve 6 rows")
	}
	if len(first.metrics) != 2 || len(first.errors) != 1 || len(second.metrics) != 1 || len(second.errors) != 2 {
		t.Errorf("halves hold %d/%d and %d/%d rows", len(first.metrics), len(first.errors), len(second.metrics), len(second.errors))
	}
	if first.id == original.id || second.id == original.id || first.id == second.id {
		t.Error("halves must get new, distinct batch ids")
	}
	if first.sdk != original.sdk {
		t.Errorf("sdk = %q, want %q", first.sdk, original.sdk)
	}
	if _, _, ok := halve(testBatch(t, 1, 0)); ok {
		t.Error("halved a single row")
	}
}

func TestBackoff(t *testing.T) {
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		base := time.Second << (attempt - 1)
		for range 1000 {
			if d := backoff(time.Second, attempt); d < base/2 || d >= base/2+base {
				t.Fatalf("backoff(attempt %d) = %v, want [%v, %v)", attempt, d, base/2, base/2+base)
			}
		}
	}
	if d := backoff(0, 1); d != 0 {
		t.Errorf("backoff with zero base = %v", d)
	}
}
