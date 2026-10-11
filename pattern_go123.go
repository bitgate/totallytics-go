//go:build go1.23

package clicktag

import "net/http"

// ServeMux stores the matched pattern on the request it was given, which is
// the one we passed down.
func matchedPattern(_ http.Handler, r *http.Request) string {
	return r.Pattern
}
