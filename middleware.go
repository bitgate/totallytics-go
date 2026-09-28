package totallytics

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

const statusClientClosedRequest = 499

type stateKey struct{}

// requestState travels in the request context, so every copy of the request
// made further down the chain shares it.
type requestState struct {
	client   *Client
	path     string
	route    atomic.Pointer[string]
	consumer atomic.Pointer[string]
	pattern  atomic.Pointer[string]
}

func stateOf(r *http.Request) *requestState {
	if r == nil {
		return nil
	}
	s, _ := r.Context().Value(stateKey{}).(*requestState)
	return s
}

// SetRoute sets the route template, such as "/users/:id", recorded for r. It
// wins over detected routes and Options.Route. Call it from any handler or
// middleware running inside a Totallytics integration.
func SetRoute(r *http.Request, route string) {
	if s := stateOf(r); s != nil && route != "" {
		s.route.Store(&route)
	}
}

// SetConsumer sets the API consumer recorded for r, for example from an
// authentication middleware. It wins over Options.Consumer.
func SetConsumer(r *http.Request, consumer string) {
	if s := stateOf(r); s != nil && consumer != "" {
		s.consumer.Store(&consumer)
	}
}

// Measurement is a request being measured, created by Client.Start. A nil
// *Measurement is valid and records nothing.
type Measurement struct {
	client    *Client
	state     *requestState
	request   *http.Request
	nested    bool
	finished  atomic.Bool
	start     time.Time
	method    string
	path      string
	userAgent string
}

// Start begins measuring r for a framework integration. Pass the returned
// request on to the handlers, since it carries the state behind SetRoute and
// SetConsumer, and call Finish once the response is complete. integration
// names the framework in the sdk field of batches, like "gin".
//
// The Measurement is nil when the client has no API key or Options.Ignore
// skips r. Inside another integration of the same client, Start returns a
// Measurement that only hands its route to the outer one, so nothing is
// counted twice.
func (c *Client) Start(r *http.Request, integration string) (*http.Request, *Measurement) {
	if c == nil || r == nil {
		return r, nil
	}
	if s := stateOf(r); s != nil && s.client == c {
		return r, &Measurement{state: s, nested: true, path: escapedPath(r)}
	}
	if c.apiKey == "" || c.skip(r) {
		return r, nil
	}

	c.useIntegration(integration)
	s := &requestState{client: c, path: escapedPath(r)}
	r = r.WithContext(context.WithValue(r.Context(), stateKey{}, s))
	return r, &Measurement{
		client:    c,
		state:     s,
		request:   r,
		start:     time.Now(),
		method:    r.Method,
		path:      s.path,
		userAgent: r.UserAgent(),
	}
}

// Finish records the request with its final status, the framework's route
// template ("" falls back to the raw path) and the error that failed it, if
// any. Only the first call counts.
func (m *Measurement) Finish(status int, route string, err error) {
	if m == nil || !m.finished.CompareAndSwap(false, true) {
		return
	}
	if m.nested {
		m.report(route)
		return
	}

	c := m.client
	defer c.log.recoverPanic("recording")
	c.record(entry{
		method:     m.method,
		path:       m.path,
		route:      m.resolveRoute(route),
		status:     status,
		durationMs: float64(time.Since(m.start)) / float64(time.Millisecond),
		startedAt:  m.start.UnixMilli(),
		userAgent:  m.userAgent,
		consumer:   m.resolveConsumer(),
		err:        err,
	})
}

// report hands an inner integration's route to the outer one, unless a
// prefix was stripped in between and the route no longer fits the path.
func (m *Measurement) report(route string) {
	if route != "" && m.path == m.state.path {
		m.state.pattern.CompareAndSwap(nil, &route)
	}
}

func (m *Measurement) resolveRoute(detected string) string {
	if route := m.state.route.Load(); route != nil {
		return *route
	}
	if m.client.route != nil {
		if route := m.client.callString(m.client.route, m.request); route != "" {
			return route
		}
	}
	if pattern := m.state.pattern.Load(); pattern != nil {
		return *pattern
	}
	return detected
}

func (m *Measurement) resolveConsumer() string {
	if consumer := m.state.consumer.Load(); consumer != nil {
		return *consumer
	}
	if m.client.consumer != nil {
		return m.client.callString(m.client.consumer, m.request)
	}
	return ""
}

func (c *Client) callString(fn func(*http.Request) string, r *http.Request) (value string) {
	defer func() {
		if p := recover(); p != nil {
			c.log.debug("callback panicked", "panic", p)
			value = ""
		}
	}()
	return fn(r)
}

func (c *Client) skip(r *http.Request) (ignored bool) {
	if c.ignore == nil {
		return false
	}
	defer func() {
		if p := recover(); p != nil {
			c.log.debug("Ignore callback panicked", "panic", p)
			ignored = false
		}
	}()
	return c.ignore(r)
}

func escapedPath(r *http.Request) string {
	if r.URL == nil {
		return "/"
	}
	if path := r.URL.EscapedPath(); path != "" {
		return path
	}
	return "/"
}

// Middleware records every request handled by next. On Go 1.23+ the pattern
// of a *http.ServeMux, like "GET /users/{id}", is sent as the route
// "/users/:id" (on Go 1.22 only when next is the ServeMux itself).
//
// Middleware between this one and the mux that replaces the request, such as
// with r.WithContext, hides the pattern; wrap the mux a second time and the
// inner Middleware passes the route out: tt.Middleware(auth(tt.Middleware(mux))).
func (c *Client) Middleware(next http.Handler) http.Handler {
	return c.Handler(next, "net/http", func(r *http.Request) string {
		return detectRoute(next, r)
	})
}

// Handler records every request handled by next like Middleware, but takes
// the route template from route once next has served the request ("" means
// the raw path). It suits routers that keep the matched route in the request
// context, such as chi. integration names the router in the sdk field.
func (c *Client) Handler(next http.Handler, integration string, route func(r *http.Request) string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r, m := c.Start(r, integration)
		switch {
		case m == nil:
			next.ServeHTTP(w, r)
		case m.nested:
			defer func() { m.Finish(0, c.callString(route, r), nil) }()
			next.ServeHTTP(w, r)
		default:
			c.serve(next, w, r, m, route)
		}
	})
}

func (c *Client) serve(next http.Handler, w http.ResponseWriter, r *http.Request, m *Measurement, route func(*http.Request) string) {
	recorder := &responseWriter{ResponseWriter: w}
	finished := false
	defer func() {
		if finished {
			return
		}
		p := recover()
		status, err := http.StatusInternalServerError, panicError(p)
		if p == nil || p == http.ErrAbortHandler {
			status, err = recorder.finalStatus(r, true), nil
		}
		m.Finish(status, c.callString(route, r), err)
		if p != nil {
			panic(p)
		}
	}()

	next.ServeHTTP(recorder, r)
	finished = true
	m.Finish(recorder.finalStatus(r, false), c.callString(route, r), nil)
}

func panicError(p any) error {
	if p == nil {
		return nil
	}
	return fmt.Errorf("panic: %v", p)
}

// responseWriter captures the status code and keeps the optional interfaces
// of the writer it wraps reachable.
type responseWriter struct {
	http.ResponseWriter
	status int
}

func (w *responseWriter) WriteHeader(code int) {
	if w.status == 0 && (code >= 200 || code == http.StatusSwitchingProtocols) {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *responseWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

func (w *responseWriter) ReadFrom(src io.Reader) (int64, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	if rf, ok := w.ResponseWriter.(io.ReaderFrom); ok {
		return rf.ReadFrom(src)
	}
	return io.Copy(struct{ io.Writer }{w.ResponseWriter}, src)
}

func (w *responseWriter) Flush() {
	_ = w.FlushError()
}

func (w *responseWriter) FlushError() error {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return http.NewResponseController(w.ResponseWriter).Flush()
}

func (w *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, rw, err := http.NewResponseController(w.ResponseWriter).Hijack()
	if err == nil && w.status == 0 {
		w.status = http.StatusSwitchingProtocols
	}
	return conn, rw, err
}

func (w *responseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// finalStatus is 499 when nothing was written because the client went away
// or the handler was aborted.
func (w *responseWriter) finalStatus(r *http.Request, aborted bool) int {
	switch {
	case w.status != 0:
		return w.status
	case aborted || r.Context().Err() != nil:
		return statusClientClosedRequest
	default:
		return http.StatusOK
	}
}
