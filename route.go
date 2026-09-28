package totallytics

import (
	"net/http"
	"strings"
	"sync"
)

// patternRoutes caches templates of registered ServeMux patterns.
var patternRoutes sync.Map

// detectRoute returns the template of the ServeMux pattern that served r, or
// "" when there is none.
func detectRoute(next http.Handler, r *http.Request) (route string) {
	defer func() {
		if recover() != nil {
			route = ""
		}
	}()

	pattern := matchedPattern(next, r)
	if pattern == "" {
		return ""
	}
	if cached, ok := patternRoutes.Load(pattern); ok {
		return cached.(string)
	}

	// Redirects report their target path instead of a pattern, and those
	// always yield "", so only real templates are cached.
	route = routeFromPattern(pattern)
	if route != "" {
		patternRoutes.Store(pattern, route)
	}
	return route
}

// routeFromPattern turns a ServeMux pattern such as "GET example.com/users/{id}"
// into the template "/users/:id". Subtree patterns like "/static/" match any
// path below them, so they yield "" and the raw path is recorded instead.
func routeFromPattern(pattern string) string {
	if i := strings.IndexAny(pattern, " \t"); i >= 0 {
		pattern = strings.TrimLeft(pattern[i:], " \t")
	}
	slash := strings.IndexByte(pattern, '/')
	if slash < 0 {
		return ""
	}
	path := pattern[slash:]
	if strings.HasSuffix(path, "/") {
		return ""
	}

	segments := strings.Split(path, "/")
	for i, segment := range segments {
		if len(segment) < 3 || segment[0] != '{' || segment[len(segment)-1] != '}' {
			continue
		}
		name := strings.TrimSuffix(segment[1:len(segment)-1], "...")
		if name == "$" {
			segments[i] = ""
		} else {
			segments[i] = ":" + name
		}
	}
	return strings.Join(segments, "/")
}
