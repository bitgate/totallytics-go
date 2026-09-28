package totallyticsecho

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/bitgate/totallytics-go"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

func ok(echo.Context) error { return nil }

func TestMiddleware(t *testing.T) {
	tt, result := capture(t)

	var (
		mu       sync.Mutex
		upstream []string
	)
	e := echo.New()
	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			err := next(c)
			if err != nil {
				mu.Lock()
				upstream = append(upstream, err.Error())
				mu.Unlock()
			}
			return err
		}
	})
	e.Use(middleware.RecoverWithConfig(middleware.RecoverConfig{
		LogErrorFunc: func(_ echo.Context, err error, _ []byte) error { return err },
	}), Middleware(tt))

	e.GET("/users/:id", func(c echo.Context) error {
		totallytics.SetConsumer(c.Request(), "acme")
		return c.String(http.StatusOK, c.Param("id"))
	})
	e.GET("/static/*", ok)
	e.GET("/teapot", func(echo.Context) error { return echo.NewHTTPError(http.StatusTeapot, "short and stout") })
	e.GET("/fail", func(echo.Context) error { return errors.New("db down") })
	e.GET("/panic", func(echo.Context) error { panic("boom") })
	e.Group("/api").GET("/items/:id", ok)

	serve(e, "/users/1", "/users/2", "/static/css/app.css", "/teapot", "/fail", "/panic", "/api/items/9", "/nope/42")

	expect(t, result(), "echo", map[string]int{
		"GET /users/:id 200 acme": 2,
		"GET /static/* 200 ":      1,
		"GET /teapot 418 ":        1,
		"GET /fail 500 ":          1,
		"GET /panic 500 ":         1,
		"GET /api/items/:id 200 ": 1,
		"GET /nope/42 404 ":       1,
	}, map[string]bool{
		"418 /teapot code=418, message=short and stout": true,
		"500 /fail db down":                             true,
		"500 /panic panic: boom":                        true,
		"404 /nope/42 code=404, message=Not Found":      true,
	})
	if len(upstream) != 3 {
		t.Errorf("outer middleware saw errors %q, want the 3 returned ones", upstream)
	}
}

func TestInsideServeMuxMiddleware(t *testing.T) {
	tt, result := capture(t)

	e := echo.New()
	e.Use(Middleware(tt))
	e.GET("/users/:id", ok)
	serve(tt.Middleware(e), "/users/1", "/users/2")

	expect(t, result(), "net/http", map[string]int{"GET /users/:id 200 ": 2}, map[string]bool{})
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
