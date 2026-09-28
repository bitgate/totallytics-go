//go:build !go1.23

package totallytics

import "net/http"

// Before Go 1.23 the matched pattern is private to ServeMux, so when we wrap
// one directly we ask it again.
func matchedPattern(next http.Handler, r *http.Request) string {
	mux, ok := next.(*http.ServeMux)
	if !ok {
		return ""
	}
	_, pattern := mux.Handler(r)
	return pattern
}
