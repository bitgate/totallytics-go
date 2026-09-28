package totallytics

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMiddlewareRecordsRequest(t *testing.T) {
	ingest := newIngest(t, nil)
	c := newTestClient(t, ingest, Options{})

	mux := http.NewServeMux()
	mux.HandleFunc("GET /users/{id}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	})
	before := time.Now().Unix() / 60 * 60
	do(c.Middleware(mux), http.MethodGet, "/users/42?token=secret", "User-Agent", "curl/8.5.0")
	after := time.Now().Unix() / 60 * 60
	shutdown(t, c)

	requests := ingest.requests()
	if len(requests) != 1 {
		t.Fatalf("got %d ingest requests, want 1", len(requests))
	}
	r := requests[0]
	if got := r.header.Get("Authorization"); got != "Bearer "+testKey {
		t.Errorf("Authorization = %q", got)
	}
	if got := r.header.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q", got)
	}
	if got := r.header.Get("User-Agent"); got != "totallytics-go/"+Version {
		t.Errorf("User-Agent = %q", got)
	}
	if bytes.Contains(r.body, []byte("secret")) {
		t.Error("the query string leaked into the batch")
	}
	if !bytes.Contains(r.body, []byte(`"errors":[]`)) {
		t.Errorf("errors must be an empty array: %s", r.body)
	}

	p := r.payload
	if p.V != 1 || len(p.BatchID) != 32 || p.SDK != "totallytics-go/"+Version+" net/http" {
		t.Errorf("envelope = v%d, batch_id %q, sdk %q", p.V, p.BatchID, p.SDK)
	}
	if len(p.Metrics) != 1 {
		t.Fatalf("got %d rows, want 1", len(p.Metrics))
	}
	row := p.Metrics[0]
	if row.Minute < before || row.Minute > after || row.Method != "GET" || row.Route != "/users/:id" || row.Status != 201 || row.Count != 1 || row.UserAgent != "curl/8.5.0" || row.Consumer != "" {
		t.Errorf("row = %+v", row)
	}
	if histogramTotal(row) != 1 || row.DurationMsSum < 0 {
		t.Errorf("histogram %v and duration %v do not add up", row.Histogram, row.DurationMsSum)
	}
}

func TestPanicIsRecordedAndRethrown(t *testing.T) {
	ingest := newIngest(t, nil)
	c := newTestClient(t, ingest, Options{})
	handler := c.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}))

	func() {
		defer func() {
			if p := recover(); p != "boom" {
				t.Errorf("recovered %v, want the original panic", p)
			}
		}()
		do(handler, http.MethodPost, "/explode")
	}()
	shutdown(t, c)

	rows, samples := ingest.metrics(), ingest.samples()
	if len(rows) != 1 || rows[0].Status != 500 || rows[0].Route != "/explode" || rows[0].Method != "POST" {
		t.Errorf("rows = %+v", rows)
	}
	if len(samples) != 1 || samples[0].Status != 500 || samples[0].Path != "/explode" || samples[0].Message != "panic: boom" {
		t.Errorf("samples = %+v", samples)
	}
}

func TestAbortedHandlersAreNotServerErrors(t *testing.T) {
	ingest := newIngest(t, nil)
	c := newTestClient(t, ingest, Options{})
	handler := c.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/streamed" {
			w.WriteHeader(http.StatusOK)
		}
		panic(http.ErrAbortHandler)
	}))
	for _, path := range []string{"/streamed", "/silent"} {
		func() {
			defer func() { _ = recover() }()
			do(handler, http.MethodGet, path)
		}()
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	gone := httptest.NewRequest(http.MethodGet, "/gone", nil).WithContext(ctx)
	c.Middleware(ok).ServeHTTP(httptest.NewRecorder(), gone)
	shutdown(t, c)

	routes := byRoute(ingest.metrics())
	if routes["/streamed"].Status != 200 || routes["/silent"].Status != 499 || routes["/gone"].Status != 499 {
		t.Errorf("statuses = %d, %d, %d", routes["/streamed"].Status, routes["/silent"].Status, routes["/gone"].Status)
	}
	for _, s := range ingest.samples() {
		if s.Status != 499 || s.Message != "" {
			t.Errorf("aborted handler produced error sample %+v", s)
		}
	}
}

func TestOptionsAndOverrides(t *testing.T) {
	ingest := newIngest(t, nil)
	c := newTestClient(t, ingest, Options{
		Ignore:   func(r *http.Request) bool { return r.URL.Path == "/healthz" },
		Consumer: func(r *http.Request) string { return r.Header.Get("X-Customer") },
		Route: func(r *http.Request) string {
			if strings.HasPrefix(r.URL.Path, "/legacy/") {
				return "/legacy/:thing"
			}
			return ""
		},
	})

	mux := http.NewServeMux()
	mux.Handle("/healthz", ok)
	mux.Handle("/legacy/", ok)
	mux.Handle("GET /items/{id}", ok)
	mux.HandleFunc("GET /orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		SetRoute(r, "/orders/:orderID")
		SetConsumer(r, "acme-override")
	})
	handler := c.Middleware(copying(mux))
	for _, path := range []string{"/healthz", "/legacy/abc", "/orders/7", "/items/9"} {
		do(handler, http.MethodGet, path, "X-Customer", "acme")
	}
	shutdown(t, c)

	routes := byRoute(ingest.metrics())
	want := map[string]string{
		"/legacy/:thing":   "acme",
		"/orders/:orderID": "acme-override",
		"/items/9":         "acme",
	}
	if len(routes) != len(want) {
		t.Errorf("routes = %v", routes)
	}
	for route, consumer := range want {
		if routes[route].Consumer != consumer {
			t.Errorf("route %q has consumer %q, want %q", route, routes[route].Consumer, consumer)
		}
	}
}

func TestCallbacksThatPanicAreContained(t *testing.T) {
	ingest := newIngest(t, nil)
	c := newTestClient(t, ingest, Options{
		Ignore:   func(*http.Request) bool { panic("ignore") },
		Consumer: func(*http.Request) string { panic("consumer") },
		Route:    func(*http.Request) string { panic("route") },
	})
	mux := http.NewServeMux()
	mux.Handle("GET /users/{id}", ok)

	if w := do(c.Middleware(mux), http.MethodGet, "/users/1"); w.Code != http.StatusOK {
		t.Fatalf("response code %d", w.Code)
	}
	shutdown(t, c)

	rows := ingest.metrics()
	if len(rows) != 1 || rows[0].Route != "/users/:id" || rows[0].Consumer != "" {
		t.Errorf("rows = %+v", rows)
	}
}

func TestNilAndKeylessClientsPassThrough(t *testing.T) {
	t.Setenv("TOTALLYTICS_API_KEY", "")
	ingest := newIngest(t, nil)
	ctx := context.Background()

	var nilClient *Client
	for _, c := range []*Client{nilClient, New(Options{Endpoint: ingest.URL})} {
		if w := do(c.Middleware(ok), http.MethodGet, "/"); w.Code != http.StatusOK {
			t.Errorf("response code %d", w.Code)
		}
		r, m := c.Start(httptest.NewRequest(http.MethodGet, "/", nil), "test")
		m.Finish(200, "/", nil)
		SetRoute(r, "/x")
		SetConsumer(r, "x")
		if err := c.Flush(ctx); err != nil {
			t.Error(err)
		}
		if err := c.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	}
	SetRoute(nil, "/x")
	if got := len(ingest.requests()); got != 0 {
		t.Errorf("got %d ingest requests, want 0", got)
	}
}

func TestAPIKeyFromEnvironment(t *testing.T) {
	t.Setenv("TOTALLYTICS_API_KEY", "  "+testKey+"\n")
	ingest := newIngest(t, nil)
	c := New(Options{Endpoint: ingest.URL})
	do(c.Middleware(ok), http.MethodGet, "/")
	shutdown(t, c)

	requests := ingest.requests()
	if len(requests) != 1 || requests[0].header.Get("Authorization") != "Bearer "+testKey {
		t.Errorf("requests = %d", len(requests))
	}
}

func TestMaxBatchRowsSplitsBatches(t *testing.T) {
	ingest := newIngest(t, nil)
	c := newTestClient(t, ingest, Options{MaxBatchRows: 1000})
	for i := range 2500 {
		route := fmt.Sprintf("/r/%d", i)
		_, m := c.Start(httptest.NewRequest(http.MethodGet, route, nil), "test")
		status, err := 200, error(nil)
		if i == 0 {
			status, err = 500, errors.New("db down")
		}
		m.Finish(status, route, err)
	}
	shutdown(t, c)

	var sizes []string
	for _, r := range ingest.requests() {
		sizes = append(sizes, fmt.Sprintf("%d/%d", len(r.payload.Metrics), len(r.payload.Errors)))
		if r.payload.SDK != "totallytics-go/"+Version+" test" {
			t.Errorf("sdk = %q", r.payload.SDK)
		}
	}
	sort.Strings(sizes)
	if got := strings.Join(sizes, " "); got != "1000/0 1000/1 500/0" {
		t.Errorf("batch sizes = %s", got)
	}
	if samples := ingest.samples(); len(samples) != 1 || samples[0].Message != "db down" {
		t.Errorf("samples = %+v", samples)
	}
}

func TestTenThousandKeysMakeTenBatches(t *testing.T) {
	ingest := newIngest(t, nil)
	c := newTestClient(t, ingest, Options{})
	for i := range maxKeys {
		route := fmt.Sprintf("/r/%d", i)
		_, m := c.Start(httptest.NewRequest(http.MethodGet, route, nil), "test")
		m.Finish(200, route, nil)
	}
	shutdown(t, c)

	requests := ingest.requests()
	if len(requests) != 10 {
		t.Fatalf("got %d batches, want 10", len(requests))
	}
	for _, r := range requests {
		if len(r.payload.Metrics) != defaultMaxBatchRows {
			t.Errorf("batch has %d rows", len(r.payload.Metrics))
		}
	}
}

func TestFlushIntervalSendsInBackground(t *testing.T) {
	ingest := newIngest(t, nil)
	c := newTestClient(t, ingest, Options{FlushInterval: 100 * time.Millisecond})
	do(c.Middleware(ok), http.MethodGet, "/tick")

	deadline := time.Now().Add(5 * time.Second)
	for len(ingest.requests()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("nothing was sent within 5s")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestShutdownGivesUpWhenContextEnds(t *testing.T) {
	block := make(chan struct{})
	ingest := newIngest(t, func(int, payload) int {
		<-block
		return http.StatusAccepted
	})
	t.Cleanup(func() { close(block) })
	c := newTestClient(t, ingest, Options{})
	do(c.Middleware(ok), http.MethodGet, "/")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := c.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Shutdown = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Shutdown took %v", elapsed)
	}
}

func TestRequestsAfterShutdownAreDropped(t *testing.T) {
	ingest := newIngest(t, nil)
	c := newTestClient(t, ingest, Options{})
	shutdown(t, c)
	do(c.Middleware(ok), http.MethodGet, "/late")
	if err := c.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(ingest.requests()); got != 0 {
		t.Errorf("got %d ingest requests, want 0", got)
	}
}

func TestConcurrentRecording(t *testing.T) {
	ingest := newIngest(t, nil)
	c := newTestClient(t, ingest, Options{MaxBatchRows: 100})
	mux := http.NewServeMux()
	mux.Handle("/", ok)
	handler := c.Middleware(mux)

	var wg sync.WaitGroup
	for g := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 500 {
				do(handler, http.MethodGet, fmt.Sprintf("/items/%d", i), "User-Agent", fmt.Sprintf("agent-%d", g%7))
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 20 {
			_ = c.Flush(context.Background())
		}
	}()
	wg.Wait()
	shutdown(t, c)

	var total int64
	for _, row := range ingest.metrics() {
		total += row.Count
	}
	if total != 32*500 {
		t.Errorf("recorded %d requests, want %d", total, 32*500)
	}
}

func TestHijackRecordsSwitchingProtocols(t *testing.T) {
	ingest := newIngest(t, nil)
	c := newTestClient(t, ingest, Options{})
	finished := make(chan struct{})
	inner := c.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, rw, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		defer conn.Close()
		rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: test\r\n\r\n")
		rw.Flush()
	}))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(finished)
		inner.ServeHTTP(w, r)
	}))
	defer server.Close()

	conn, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprint(conn, "GET /ws HTTP/1.1\r\nHost: test\r\nConnection: Upgrade\r\nUpgrade: test\r\n\r\n")
	status, err := bufio.NewReader(conn).ReadString('\n')
	conn.Close()
	if err != nil || !strings.HasPrefix(status, "HTTP/1.1 101") {
		t.Fatalf("status line %q, %v", status, err)
	}
	<-finished
	shutdown(t, c)

	rows := ingest.metrics()
	if len(rows) != 1 || rows[0].Status != 101 || rows[0].Route != "/ws" {
		t.Errorf("rows = %+v", rows)
	}
}

func TestOptionalWriterInterfacesPassThrough(t *testing.T) {
	ingest := newIngest(t, nil)
	c := newTestClient(t, ingest, Options{})
	w := httptest.NewRecorder()
	c.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.Copy(w, strings.NewReader("chunk")); err != nil {
			t.Errorf("copy: %v", err)
		}
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Errorf("flush: %v", err)
		}
	})).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/stream", nil))
	shutdown(t, c)

	if !w.Flushed || w.Body.String() != "chunk" {
		t.Errorf("flushed %v, body %q", w.Flushed, w.Body.String())
	}
	if rows := ingest.metrics(); len(rows) != 1 || rows[0].Status != 200 {
		t.Errorf("rows = %+v", rows)
	}
}
