# totallytics-go

[![Go Reference](https://pkg.go.dev/badge/github.com/bitgate/totallytics-go.svg)](https://pkg.go.dev/github.com/bitgate/totallytics-go)

Server-side API analytics for Go services, for [Totallytics](https://totallytics.com). Requests are counted per minute in memory and sent in small batches from a background goroutine, so a request costs one map update. Request and response bodies, headers and query strings never leave your server.

```sh
go get github.com/bitgate/totallytics-go
```

Go 1.22 or newer, no dependencies. Create an API key in your Totallytics dashboard and set `TOTALLYTICS_API_KEY`.

## net/http

```go
tt := totallytics.New(totallytics.Options{}) // reads TOTALLYTICS_API_KEY

mux := http.NewServeMux()
mux.HandleFunc("GET /users/{id}", getUser) // recorded as GET /users/:id

server := &http.Server{Addr: ":8080", Handler: tt.Middleware(mux)}
```

The matched `ServeMux` pattern becomes the route. On Go 1.23+ that works anywhere in front of the mux, as long as the middleware in between passes the request on as is; on Go 1.22 `Middleware` has to wrap the `*http.ServeMux` itself. When no pattern is found the raw path is sent and Totallytics templates id-like segments on its side.

Middleware that replaces the request (`r.WithContext`) hides the pattern. Wrap the mux a second time and the inner one hands the route out, without counting the request twice:

```go
handler := tt.Middleware(auth(tt.Middleware(mux)))
```

## chi, gin and echo

The adapters are separate modules, so the core stays dependency-free. Register them before your routes to see 404s too.

```go
// go get github.com/bitgate/totallytics-go/chi
r := chi.NewRouter()
r.Use(totallyticschi.Middleware(tt))
r.Get("/users/{id}", getUser) // recorded as /users/:id
```

```go
// go get github.com/bitgate/totallytics-go/gin
engine := gin.New()
engine.Use(gin.Recovery(), totallyticsgin.Middleware(tt))
engine.GET("/users/:id", getUser)
```

```go
// go get github.com/bitgate/totallytics-go/echo
e := echo.New()
e.Use(middleware.Recover(), totallyticsecho.Middleware(tt))
e.GET("/users/:id", getUser)
```

Panics are recorded as 500 with their message and re-panicked for the framework's recovery. Errors returned by echo handlers or added with gin's `c.Error` are sent along with 4xx and 5xx samples.

For other routers, `tt.Handler(next, "name", routeFunc)` takes the route from your function after the request, and `Client.Start` with `Measurement.Finish` covers anything else.

## Consumers and routes

```go
tt := totallytics.New(totallytics.Options{
	Consumer: func(r *http.Request) string { return r.Header.Get("X-Customer-Id") },
	Ignore:   func(r *http.Request) bool { return r.URL.Path == "/healthz" },
})
```

From a handler or auth middleware, `totallytics.SetConsumer(r, customerID)` and `totallytics.SetRoute(r, "/users/:id")` win over the options and the detected route.

## Graceful shutdown

Stop the server first so in-flight requests are recorded, then flush:

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()
go server.ListenAndServe()
<-ctx.Done()

shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
server.Shutdown(shutdown)
tt.Shutdown(shutdown)
```

## Serverless

Platforms that freeze the process between requests (AWS Lambda, Cloud Functions, Cloud Run with request-based CPU) also freeze the background flusher. Call `tt.Flush(ctx)` before the invocation returns.

## Options

| Option | Default | |
|---|---|---|
| `APIKey` | `$TOTALLYTICS_API_KEY` | Nothing is recorded without a key. |
| `Endpoint` | `https://totallytics.com/api/ingest` | |
| `Consumer` | | `func(*http.Request) string` naming the API consumer, like a customer or key id. |
| `Route` | detected | `func(*http.Request) string` overriding the route; `""` keeps the detected one. |
| `Ignore` | | `func(*http.Request) bool` skipping requests such as health checks. |
| `MaxBatchRows` | 1000 | Metric rows per batch, 1 to 5000. A full buffer is sent right away. |
| `FlushInterval` | 10s | 100ms to 1h. |
| `Logger` | `slog.Default()` | Debug diagnostics, plus one warning if the API key is rejected. |

## What is sent

Per minute, method, route, status, client User-Agent and consumer: a request count, the summed duration and a latency histogram. Each batch also carries up to 50 5xx and 20 4xx samples with the path (no query string), status, duration and error message.

Failed batches are retried up to 3 times with backoff and the same batch id, so the server drops duplicates. Oversized batches are split, and a rejected API key is logged once and not retried. The wire format is shared with [totallytics-js](https://github.com/bitgate/totallytics-js), see its [WIRE.md](https://github.com/bitgate/totallytics-js/blob/master/WIRE.md).

## License

MIT
