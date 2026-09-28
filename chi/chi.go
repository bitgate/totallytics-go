// Package totallyticschi records requests to a chi router for Totallytics
// API analytics, with chi route patterns as templates.
//
//	r := chi.NewRouter()
//	r.Use(totallyticschi.Middleware(tt))
//	r.Get("/users/{id}", getUser) // recorded as "/users/:id"
package totallyticschi

import (
	"net/http"
	"strings"

	"github.com/bitgate/totallytics-go"
	"github.com/go-chi/chi/v5"
)

// Middleware records every request the router handles, including 404s and
// panics. Register it first with r.Use, since chi only sets the route pattern
// inside the router.
func Middleware(tt *totallytics.Client) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return tt.Handler(next, "chi", route)
	}
}

func route(r *http.Request) string {
	rctx := chi.RouteContext(r.Context())
	if rctx == nil {
		return ""
	}
	return template(rctx.RoutePattern())
}

// template turns chi parameters like "{id}" and "{id:[0-9]{3}}" into ":id".
func template(pattern string) string {
	if !strings.Contains(pattern, "{") {
		return pattern
	}

	var b strings.Builder
	for i := 0; i < len(pattern); i++ {
		if pattern[i] != '{' {
			b.WriteByte(pattern[i])
			continue
		}
		end := closingBrace(pattern, i)
		if end < 0 {
			b.WriteString(pattern[i:])
			break
		}
		name, _, _ := strings.Cut(pattern[i+1:end], ":")
		b.WriteString(":" + name)
		i = end
	}
	return b.String()
}

func closingBrace(pattern string, open int) int {
	depth := 0
	for i := open; i < len(pattern); i++ {
		switch pattern[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}
