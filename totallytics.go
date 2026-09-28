// Package totallytics records server-side API analytics for Totallytics
// (https://totallytics.com).
//
// Requests are aggregated per minute in memory and sent in batches from a
// background goroutine, so a request only pays for a map update. Request and
// response bodies, headers and query strings are never sent.
//
//	tt := totallytics.New(totallytics.Options{}) // reads TOTALLYTICS_API_KEY
//	defer tt.Shutdown(context.Background())
//
//	mux := http.NewServeMux()
//	mux.HandleFunc("GET /users/{id}", getUser)
//	http.ListenAndServe(":8080", tt.Middleware(mux))
//
// Adapters for chi, gin and echo are separate modules under
// github.com/bitgate/totallytics-go, so this package has no dependencies.
package totallytics

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultEndpoint is the Totallytics ingest URL.
const DefaultEndpoint = "https://totallytics.com/api/ingest"

const (
	defaultMaxBatchRows  = 1000
	maxMaxBatchRows      = 5000
	defaultFlushInterval = 10 * time.Second
	minFlushInterval     = 100 * time.Millisecond
	maxFlushInterval     = time.Hour

	maxSDK       = 64
	maxPath      = 512
	maxUserAgent = 512
	maxConsumer  = 128
	maxMessage   = 1000
)

// Options configures a Client. The zero value is ready to use.
type Options struct {
	// APIKey authenticates batches. It defaults to the TOTALLYTICS_API_KEY
	// environment variable; without a key nothing is recorded.
	APIKey string

	// Endpoint is the ingest URL. It defaults to DefaultEndpoint.
	Endpoint string

	// Consumer names the API consumer of a request, such as a customer or
	// key id. It sees the request as it entered the integration; SetConsumer
	// wins over it.
	Consumer func(r *http.Request) string

	// Route overrides the detected route template of a request. Returning ""
	// keeps the detected one. SetRoute wins over it.
	Route func(r *http.Request) string

	// Ignore skips requests, such as health checks, before they are measured.
	Ignore func(r *http.Request) bool

	// MaxBatchRows caps the metric rows per batch, from 1 to 5000 (default
	// 1000). A buffer that reaches it is sent right away.
	MaxBatchRows int

	// FlushInterval is how often buffered metrics are sent, from 100ms to 1h
	// (default 10s).
	FlushInterval time.Duration

	// Logger receives diagnostics at debug level and a one-time warning when
	// the API key is rejected. It defaults to slog.Default().
	Logger *slog.Logger
}

// Client aggregates requests and delivers them to the ingest API. It is safe
// for concurrent use. Create one per process and call Shutdown before exit.
type Client struct {
	apiKey        string
	consumer      func(*http.Request) string
	route         func(*http.Request) string
	ignore        func(*http.Request) bool
	maxBatchRows  int
	flushInterval time.Duration
	log           logger
	transport     *transport
	integration   atomic.Pointer[string]

	mu     sync.Mutex
	buffer *aggregator
	full   []*aggregator
	closed bool

	// flushMu makes Flush wait for buffers the loop is still sealing.
	flushMu sync.Mutex

	wake     chan struct{}
	stop     chan struct{}
	stopped  chan struct{}
	stopOnce sync.Once
}

// New creates a Client and starts its background flusher.
func New(opts Options) *Client {
	return newClient(opts, nil)
}

func newClient(opts Options, configure func(*Client)) *Client {
	apiKey := strings.TrimSpace(opts.APIKey)
	if apiKey == "" {
		apiKey = strings.TrimSpace(os.Getenv("TOTALLYTICS_API_KEY"))
	}
	endpoint := opts.Endpoint
	if endpoint == "" {
		endpoint = DefaultEndpoint
	}

	log := logger{opts.Logger}
	c := &Client{
		apiKey:        apiKey,
		consumer:      opts.Consumer,
		route:         opts.Route,
		ignore:        opts.Ignore,
		maxBatchRows:  clamp(opts.MaxBatchRows, 1, maxMaxBatchRows, defaultMaxBatchRows),
		flushInterval: clamp(opts.FlushInterval, minFlushInterval, maxFlushInterval, defaultFlushInterval),
		log:           log,
		transport:     newTransport(endpoint, apiKey, log),
		buffer:        &aggregator{},
		wake:          make(chan struct{}, 1),
		stop:          make(chan struct{}),
		stopped:       make(chan struct{}),
	}
	if configure != nil {
		configure(c)
	}

	if apiKey == "" {
		log.debug("no API key (set TOTALLYTICS_API_KEY or Options.APIKey), requests are not recorded")
		close(c.stopped)
		return c
	}
	go c.loop()
	return c
}

// Flush sends everything buffered and waits until in-flight batches are
// delivered or dropped, or until ctx is done.
func (c *Client) Flush(ctx context.Context) error {
	if c == nil || c.apiKey == "" {
		return nil
	}
	c.flush(true)
	if err := c.transport.settle(ctx); err != nil {
		return fmt.Errorf("totallytics: flush: %w", err)
	}
	return nil
}

// Shutdown stops the background flusher, sends everything buffered and waits
// for delivery until ctx is done. Requests that finish afterwards are not
// recorded, so call it after http.Server.Shutdown returns.
func (c *Client) Shutdown(ctx context.Context) error {
	if c == nil || c.apiKey == "" {
		return nil
	}
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	c.stopOnce.Do(func() { close(c.stop) })
	defer c.transport.abort()

	select {
	case <-c.stopped:
	case <-ctx.Done():
		return fmt.Errorf("totallytics: shutdown: %w", ctx.Err())
	}
	c.flush(true)
	if err := c.transport.settle(ctx); err != nil {
		return fmt.Errorf("totallytics: shutdown: %w", err)
	}
	return nil
}

func (c *Client) loop() {
	defer close(c.stopped)
	ticker := time.NewTicker(c.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.flush(true)
		case <-c.wake:
			c.flush(false)
		case <-c.stop:
			return
		}
	}
}

// flush sends the buffers that filled up and, when all is set, the current one.
func (c *Client) flush(all bool) {
	defer c.log.recoverPanic("flush")
	c.flushMu.Lock()
	defer c.flushMu.Unlock()

	c.mu.Lock()
	buffers := c.full
	c.full = nil
	if all && c.buffer.size() > 0 {
		buffers = append(buffers, c.buffer)
		c.buffer = &aggregator{}
	}
	c.mu.Unlock()

	sdk := c.sdk()
	for _, buffer := range buffers {
		for _, b := range seal(buffer, sdk, c.maxBatchRows) {
			c.transport.deliver(b)
		}
	}
}

// record adds a finished request to the buffer. A buffer that reaches
// maxBatchRows is swapped out here and sent by the loop.
func (c *Client) record(e entry) {
	m, ok := measure(e)
	if !ok {
		return
	}

	c.mu.Lock()
	if c.closed || !c.buffer.add(m) || c.buffer.size() < c.maxBatchRows {
		c.mu.Unlock()
		return
	}
	c.full = append(c.full, c.buffer)
	c.buffer = &aggregator{}
	c.mu.Unlock()

	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// seal drains buffer into batches of at most maxRows metric rows. The error
// samples ride along with the first batch.
func seal(buffer *aggregator, sdk string, maxRows int) []*batch {
	metrics, samples := buffer.drain()
	batches := make([]*batch, 0, (len(metrics)+maxRows-1)/maxRows)
	for start := 0; start < len(metrics); start += maxRows {
		var rowSamples []errorRow
		if start == 0 {
			rowSamples = samples
		}
		batches = append(batches, newBatch(sdk, metrics[start:min(start+maxRows, len(metrics))], rowSamples))
	}
	return batches
}

func (c *Client) sdk() string {
	sdk := "totallytics-go/" + Version
	if integration := c.integration.Load(); integration != nil {
		sdk += " " + *integration
	}
	return clip(sdk, maxSDK)
}

// useIntegration names the first integration that records through c.
func (c *Client) useIntegration(name string) {
	if name != "" && c.integration.Load() == nil {
		c.integration.CompareAndSwap(nil, &name)
	}
}

// entry is one finished request as reported by an integration.
type entry struct {
	method     string
	path       string
	route      string
	status     int
	durationMs float64
	startedAt  int64
	userAgent  string
	consumer   string
	err        error
}

// measure validates and clips e exactly like the JavaScript SDK does.
func measure(e entry) (measurement, bool) {
	if e.status < 100 || e.status > 599 {
		return measurement{}, false
	}

	duration := e.durationMs
	if math.IsNaN(duration) || math.IsInf(duration, 0) || duration <= 0 {
		duration = 0
	}
	path := e.path
	if path == "" {
		path = "/"
	}
	path = clip(path, maxPath)
	method := "GET"
	if e.method != "" {
		method = strings.ToUpper(e.method)
	}
	route := path
	if e.route != "" {
		route = clip(e.route, maxPath)
	}

	m := measurement{
		ts:         e.startedAt,
		method:     method,
		route:      route,
		path:       path,
		status:     e.status,
		durationMs: duration,
		userAgent:  clip(e.userAgent, maxUserAgent),
		consumer:   clip(e.consumer, maxConsumer),
	}
	if e.err != nil {
		m.message = clip(e.err.Error(), maxMessage)
	}
	return m, true
}

func clamp[T ~int | ~int64](value, low, high, fallback T) T {
	if value == 0 {
		return fallback
	}
	return min(max(value, low), high)
}

// clip cuts s to limit UTF-16 code units without splitting a character, the
// way JavaScript measures string length.
func clip(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	units := 0
	for i, r := range s {
		width := 1
		if r > 0xFFFF {
			width = 2
		}
		if units+width > limit {
			return s[:i]
		}
		units += width
	}
	return s
}

type logger struct {
	base *slog.Logger
}

func (l logger) get() *slog.Logger {
	if l.base != nil {
		return l.base
	}
	return slog.Default()
}

func (l logger) debug(msg string, args ...any) {
	l.get().Debug("totallytics: "+msg, args...)
}

func (l logger) warn(msg string, args ...any) {
	l.get().Warn("totallytics: "+msg, args...)
}

// recoverPanic keeps a panic inside the SDK from reaching the caller. It must
// be deferred directly.
func (l logger) recoverPanic(scope string) {
	if p := recover(); p != nil {
		l.debug(scope+" panicked", "panic", p)
	}
}
