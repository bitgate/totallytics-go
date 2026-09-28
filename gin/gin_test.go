package totallyticsgin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/bitgate/totallytics-go"
	"github.com/gin-gonic/gin"
)

func init() {
	gin.SetMode(gin.TestMode)
}

func ok(*gin.Context) {}

func TestMiddleware(t *testing.T) {
	tt, result := capture(t)

	engine := gin.New()
	engine.Use(gin.RecoveryWithWriter(io.Discard), Middleware(tt))
	engine.GET("/users/:id", func(c *gin.Context) {
		totallytics.SetConsumer(c.Request, "acme")
		c.String(http.StatusOK, c.Param("id"))
	})
	engine.GET("/static/*filepath", ok)
	engine.GET("/panic", func(*gin.Context) { panic("boom") })
	engine.GET("/fail", func(c *gin.Context) {
		_ = c.Error(errors.New("upstream timed out"))
		c.AbortWithStatus(http.StatusBadGateway)
	})
	engine.GET("/created", func(c *gin.Context) { c.Status(http.StatusCreated) })
	engine.Group("/api/v1").GET("/items/:id", ok)

	serve(engine, "/users/1", "/users/2", "/static/css/app.css", "/panic", "/fail", "/created", "/api/v1/items/9", "/nope/42")

	expect(t, result(), "gin", map[string]int{
		"GET /users/:id 200 acme":    2,
		"GET /static/*filepath 200 ": 1,
		"GET /panic 500 ":            1,
		"GET /fail 502 ":             1,
		"GET /created 201 ":          1,
		"GET /api/v1/items/:id 200 ": 1,
		"GET /nope/42 404 ":          1,
	}, map[string]bool{
		"500 /panic panic: boom":       true,
		"502 /fail upstream timed out": true,
		"404 /nope/42 ":                true,
	})
}

func TestInsideServeMuxMiddleware(t *testing.T) {
	tt, result := capture(t)

	engine := gin.New()
	engine.Use(Middleware(tt))
	engine.GET("/users/:id", ok)
	serve(tt.Middleware(engine), "/users/1", "/users/2")

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
