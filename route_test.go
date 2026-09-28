package totallytics

import (
	"context"
	"net/http"
	"testing"
)

func TestRouteFromPattern(t *testing.T) {
	cases := map[string]string{
		"GET /users/{id}":                  "/users/:id",
		"/users/{id}/posts/{post}":         "/users/:id/posts/:post",
		"POST example.com/files/{path...}": "/files/:path",
		"GET\t/tabbed/{x}":                 "/tabbed/:x",
		"/{$}":                             "/",
		"GET /users/{$}":                   "/users/",
		"/plain":                           "/plain",
		"/static/":                         "",
		"/":                                "",
		"example.com/":                     "",
		"":                                 "",
	}
	for pattern, want := range cases {
		if got := routeFromPattern(pattern); got != want {
			t.Errorf("routeFromPattern(%q) = %q, want %q", pattern, got, want)
		}
	}
}

type copiedKey struct{}

func copying(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), copiedKey{}, true)))
	})
}

func TestServeMuxRoutes(t *testing.T) {
	ingest := newIngest(t, nil)
	c := newTestClient(t, ingest, Options{})

	mux := http.NewServeMux()
	mux.Handle("GET /users/{id}", ok)
	mux.Handle("GET /files/{path...}", ok)
	mux.Handle("/static/", ok)
	mux.Handle("GET /{$}", ok)

	inner := http.NewServeMux()
	inner.Handle("GET /things/{id}", ok)
	outer := http.NewServeMux()
	outer.Handle("/api/", http.StripPrefix("/api", c.Middleware(inner)))

	requests := []struct {
		handler http.Handler
		path    string
	}{
		{c.Middleware(mux), "/users/7"},
		{c.Middleware(mux), "/files/a/b.txt"},
		{c.Middleware(mux), "/static/app.css"},
		{c.Middleware(mux), "/"},
		{c.Middleware(mux), "/nope/1"},
		{c.Middleware(copying(mux)), "/users/8"},
		{c.Middleware(copying(c.Middleware(mux))), "/users/9"},
		{c.Middleware(outer), "/api/things/7"},
	}
	for _, r := range requests {
		do(r.handler, http.MethodGet, r.path)
	}
	shutdown(t, c)

	want := map[string]int64{
		"/users/:id":      2,
		"/files/:path":    1,
		"/static/app.css": 1,
		"/":               1,
		"/nope/1":         1,
		"/users/8":        1,
		"/api/things/7":   1,
	}
	got := byRoute(ingest.metrics())
	if len(got) != len(want) {
		t.Errorf("routes = %v, want %v", got, want)
	}
	for route, count := range want {
		if got[route].Count != count {
			t.Errorf("route %q counted %d times, want %d", route, got[route].Count, count)
		}
	}
	if got["/nope/1"].Status != http.StatusNotFound {
		t.Errorf("unmatched request recorded as %d", got["/nope/1"].Status)
	}
}
