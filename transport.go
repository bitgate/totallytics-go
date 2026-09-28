package totallytics

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	mathrand "math/rand/v2"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxAttempts    = 3
	maxPending     = 20
	attemptTimeout = 10 * time.Second
	backoffBase    = time.Second
	maxReplyBytes  = 64 << 10
)

const userAgent = "totallytics-go/" + Version

// unauthorizedWarned makes the rejected key warning fire once per process.
var unauthorizedWarned atomic.Bool

type outcome int

const (
	outcomeDone outcome = iota
	outcomeDrop
	outcomeRetry
	outcomeSplit
)

type payload struct {
	V       int         `json:"v"`
	BatchID string      `json:"batch_id"`
	SDK     string      `json:"sdk,omitempty"`
	Metrics []metricRow `json:"metrics"`
	Errors  []errorRow  `json:"errors"`
}

// batch is serialized once, so every retry sends the same bytes and the
// server can dedup on its id.
type batch struct {
	id      string
	sdk     string
	metrics []metricRow
	errors  []errorRow
	body    []byte
}

func newBatch(sdk string, metrics []metricRow, samples []errorRow) *batch {
	if metrics == nil {
		metrics = []metricRow{}
	}
	if samples == nil {
		samples = []errorRow{}
	}
	return &batch{id: randomID(), sdk: sdk, metrics: metrics, errors: samples}
}

// encode serializes the batch on first use, in its delivery goroutine.
func (b *batch) encode() error {
	if b.body != nil {
		return nil
	}
	body, err := json.Marshal(payload{V: 1, BatchID: b.id, SDK: b.sdk, Metrics: b.metrics, Errors: b.errors})
	if err != nil {
		return fmt.Errorf("encode batch %s: %w", b.id, err)
	}
	b.body = body
	return nil
}

// halve splits a batch the ingest API rejected as too large into two new
// batches with new ids.
func halve(b *batch) (first, second *batch, ok bool) {
	if len(b.metrics)+len(b.errors) < 2 {
		return nil, nil, false
	}
	metricsCut := (len(b.metrics) + 1) / 2
	errorsCut := len(b.errors) / 2
	first = newBatch(b.sdk, b.metrics[:metricsCut], b.errors[:errorsCut])
	second = newBatch(b.sdk, b.metrics[metricsCut:], b.errors[errorsCut:])
	return first, second, true
}

func randomID() string {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		for i := range id {
			id[i] = byte(mathrand.Uint32())
		}
	}
	return hex.EncodeToString(id[:])
}

type delivery struct {
	batch   *batch
	dropped atomic.Bool
	done    chan struct{}
}

type transport struct {
	endpoint    string
	apiKey      string
	client      *http.Client
	log         logger
	ctx         context.Context
	abort       context.CancelFunc
	timeout     time.Duration
	backoffBase time.Duration

	mu      sync.Mutex
	pending []*delivery
	running map[*delivery]struct{}
}

func newTransport(endpoint, apiKey string, log logger) *transport {
	ctx, abort := context.WithCancel(context.Background())
	return &transport{
		endpoint:    endpoint,
		apiKey:      apiKey,
		client:      &http.Client{},
		log:         log,
		ctx:         ctx,
		abort:       abort,
		timeout:     attemptTimeout,
		backoffBase: backoffBase,
		running:     make(map[*delivery]struct{}),
	}
}

// deliver sends b in the background. Beyond maxPending batches the oldest one
// is dropped after its current attempt.
func (t *transport) deliver(b *batch) *delivery {
	d := &delivery{batch: b, done: make(chan struct{})}

	t.mu.Lock()
	t.running[d] = struct{}{}
	t.pending = append(t.pending, d)
	var evicted []*delivery
	for len(t.pending) > maxPending {
		oldest := t.pending[0]
		oldest.dropped.Store(true)
		evicted = append(evicted, oldest)
		t.pending = slices.Delete(t.pending, 0, 1)
	}
	t.mu.Unlock()

	for _, oldest := range evicted {
		t.log.debug("batch dropped", "batch", oldest.batch.id, "reason", fmt.Sprintf("more than %d batches pending", maxPending))
	}
	go t.run(d)
	return d
}

// settle waits for the deliveries running right now, or until ctx is done.
func (t *transport) settle(ctx context.Context) error {
	t.mu.Lock()
	waits := make([]chan struct{}, 0, len(t.running))
	for d := range t.running {
		waits = append(waits, d.done)
	}
	t.mu.Unlock()

	for _, done := range waits {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// forget takes d out of the pending queue and, once it finished, out of running.
func (t *transport) forget(d *delivery, finished bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if i := slices.Index(t.pending, d); i >= 0 {
		t.pending = slices.Delete(t.pending, i, i+1)
	}
	if finished {
		delete(t.running, d)
	}
}

func (t *transport) run(d *delivery) {
	defer close(d.done)
	defer t.forget(d, true)
	defer t.log.recoverPanic("delivery")

	if err := d.batch.encode(); err != nil {
		t.log.debug("batch dropped", "batch", d.batch.id, "error", err)
		return
	}
	for attempt := 1; ; attempt++ {
		result := t.send(d.batch)
		if d.dropped.Load() || result == outcomeDone || result == outcomeDrop {
			return
		}
		if result == outcomeSplit {
			t.split(d)
			return
		}
		if attempt >= maxAttempts {
			t.log.debug("batch dropped", "batch", d.batch.id, "reason", fmt.Sprintf("no success after %d attempts", maxAttempts))
			return
		}
		if !t.sleep(backoff(t.backoffBase, attempt)) || d.dropped.Load() {
			return
		}
	}
}

// split replaces a batch rejected with 413 by its two halves and waits for them.
func (t *transport) split(d *delivery) {
	t.forget(d, false)
	first, second, ok := halve(d.batch)
	if !ok {
		t.log.debug("batch dropped", "batch", d.batch.id, "reason", "too large and cannot be split")
		return
	}
	a, b := t.deliver(first), t.deliver(second)
	<-a.done
	<-b.done
}

func (t *transport) send(b *batch) outcome {
	ctx, cancel := context.WithTimeout(t.ctx, t.timeout)
	defer cancel()

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, t.endpoint, bytes.NewReader(b.body))
	if err != nil {
		t.log.debug("batch dropped", "batch", b.id, "error", err)
		return outcomeDrop
	}
	request.Header.Set("Authorization", "Bearer "+t.apiKey)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", userAgent)

	response, err := t.client.Do(request)
	if err != nil {
		t.log.debug("batch failed, retrying", "batch", b.id, "error", err)
		return outcomeRetry
	}
	reply, _ := io.ReadAll(io.LimitReader(response.Body, maxReplyBytes))
	response.Body.Close()

	status := response.StatusCode
	switch {
	case status >= 200 && status < 300:
		t.reportRejected(b, reply)
		return outcomeDone
	case status == http.StatusUnauthorized:
		if unauthorizedWarned.CompareAndSwap(false, true) {
			t.log.warn("ingest rejected the API key (401), analytics are being dropped. Check TOTALLYTICS_API_KEY.")
		}
		return outcomeDrop
	case status == http.StatusRequestEntityTooLarge:
		return outcomeSplit
	case status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500:
		t.log.debug("batch failed, retrying", "batch", b.id, "status", status, "reply", clip(string(reply), 200))
		return outcomeRetry
	default:
		t.log.debug("batch dropped", "batch", b.id, "status", status, "reply", clip(string(reply), 200))
		return outcomeDrop
	}
}

func (t *transport) reportRejected(b *batch, reply []byte) {
	var accepted struct {
		Rejected int `json:"rejected"`
	}
	if json.Unmarshal(reply, &accepted) == nil && accepted.Rejected > 0 {
		t.log.debug("server rejected rows", "batch", b.id, "rejected", accepted.Rejected)
	}
}

func (t *transport) sleep(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-t.ctx.Done():
		return false
	}
}

// backoff waits base/2 plus up to base, doubling base with every attempt.
func backoff(base time.Duration, attempt int) time.Duration {
	scaled := base << (attempt - 1)
	if scaled <= 0 {
		return 0
	}
	return scaled/2 + mathrand.N(scaled)
}
