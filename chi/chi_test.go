package totallyticschi

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/bitgate/totallytics-go"
	"github.com/go-chi/chi/v5"
)

var ok = http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})

func recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recover() != nil {
				w.WriteHeader(http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func TestMiddleware(t *testing.T) {
	tt, result := capture(t)

	admin := chi.NewRouter()
	admin.Get("/users/{id}", ok)

	r := chi.NewRouter()
	r.Use(recoverer, Middleware(tt))
	r.Get("/users/{id}", func(_ http.ResponseWriter, r *http.Request) {
		totallytics.SetConsumer(r, "acme")
	})
	r.Get("/files/{name:[a-z]+}.{ext}", ok)
	r.Get("/codes/{code:[0-9]{3}}", ok)
	r.Get("/static/*", ok)
	r.Get("/panic", func(http.ResponseWriter, *http.Request) { panic("boom") })
	r.Route("/api", func(r chi.Router) {
		r.Get("/items/{id}", ok)
	})
	r.Mount("/admin", admin)

	serve(r, "/users/1", "/users/2", "/files/report.pdf", "/codes/404", "/static/css/app.css",
		"/panic", "/api/items/9", "/admin/users/3", "/nope/42", "/admin/nope")

	expect(t, result(), "chi", map[string]int{
		"GET /users/:id 200 acme":    2,
		"GET /files/:name.:ext 200 ": 1,
		"GET /codes/:code 200 ":      1,
		"GET /static/* 200 ":         1,
		"GET /panic 500 ":            1,
		"GET /api/items/:id 200 ":    1,
		"GET /admin/users/:id 200 ":  1,
		"GET /nope/42 404 ":          1,
		"GET /admin/* 404 ":          1,
	}, map[string]bool{
		"500 /panic panic: boom": true,
		"404 /nope/42 ":          true,
		"404 /admin/* ":          true,
	})
}

func TestInsideServeMuxMiddleware(t *testing.T) {
	tt, result := capture(t)

	r := chi.NewRouter()
	r.Use(Middleware(tt))
	r.Get("/users/{id}", ok)
	serve(tt.Middleware(r), "/users/1", "/users/2")

	expect(t, result(), "net/http", map[string]int{"GET /users/:id 200 ": 2}, map[string]bool{})
}

func TestTemplate(t *testing.T) {
	cases := map[string]string{
		"/users/{id}":              "/users/:id",
		"/users/{id:[0-9]+}/posts": "/users/:id/posts",
		"/codes/{code:[0-9]{3}}":   "/codes/:code",
		"/{month}-{day}-{year}":    "/:month-:day-:year",
		"/static/*":                "/static/*",
		"/broken/{id":              "/broken/{id",
		"":                         "",
	}
	for pattern, want := range cases {
		if got := template(pattern); got != want {
			t.Errorf("template(%q) = %q, want %q", pattern, got, want)
		}
	}
}

type metric struct {
	Method   string `json:"method"`
	Route    string `json:"route"`
	Status   int    `json:"status"`
	Count    int    `json:"count"`
	Consumer string `json:"consumer"`
}

type sample struct {
	Route   string `json:"route"`
	Status  int    `json:"status"`
	Message string `json:"message"`
}

type sent struct {
	SDK     string   `json:"sdk"`
	Metrics []metric `json:"metrics"`
	Errors  []sample `json:"errors"`
}

// capture returns a client delivering to a local ingest server and a function
// that shuts the client down and returns everything it sent.
func capture(t *testing.T) (*totallytics.Client, func() sent) {
	t.Helper()
	var (
		mu       sync.Mutex
		received sent
	)
	ingest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var batch sent
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			t.Errorf("decode batch: %v", err)
		}
		mu.Lock()
		received.SDK = batch.SDK
		received.Metrics = append(received.Metrics, batch.Metrics...)
		received.Errors = append(received.Errors, batch.Errors...)
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(ingest.Close)

	tt := totallytics.New(totallytics.Options{APIKey: "tt_" + strings.Repeat("ab", 24), Endpoint: ingest.URL})
	return tt, func() sent {
		t.Helper()
		if err := tt.Shutdown(context.Background()); err != nil {
			t.Fatalf("shutdown: %v", err)
		}
		mu.Lock()
		defer mu.Unlock()
		return received
	}
}

func (s sent) counts() map[string]int {
	counts := map[string]int{}
	for _, m := range s.Metrics {
		counts[fmt.Sprintf("%s %s %d %s", m.Method, m.Route, m.Status, m.Consumer)] += m.Count
	}
	return counts
}

func (s sent) samples() map[string]bool {
	samples := map[string]bool{}
	for _, e := range s.Errors {
		samples[fmt.Sprintf("%d %s %s", e.Status, e.Route, e.Message)] = true
	}
	return samples
}

func serve(handler http.Handler, paths ...string) {
	for _, path := range paths {
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))
	}
}

func expect(t *testing.T, got sent, sdk string, counts map[string]int, samples map[string]bool) {
	t.Helper()
	if got.SDK != "totallytics-go/"+totallytics.Version+" "+sdk {
		t.Errorf("sdk = %q", got.SDK)
	}
	if !maps.Equal(got.counts(), counts) {
		t.Errorf("metrics\n got %v\nwant %v", got.counts(), counts)
	}
	if !maps.Equal(got.samples(), samples) {
		t.Errorf("error samples\n got %v\nwant %v", got.samples(), samples)
	}
}
