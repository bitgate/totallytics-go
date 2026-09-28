// Package totallyticsgin records requests to a gin engine for Totallytics API
// analytics, with gin routes as templates.
//
//	engine := gin.New()
//	engine.Use(gin.Recovery(), totallyticsgin.Middleware(tt))
//	engine.GET("/users/:id", getUser)
package totallyticsgin

import (
	"fmt"
	"net/http"

	"github.com/bitgate/totallytics-go"
	"github.com/gin-gonic/gin"
)

const statusClientClosedRequest = 499

// Middleware records every request the engine handles, including 404s and
// panics, which it records as 500 and re-panics for gin.Recovery. The last
// error added with c.Error is sent with 4xx and 5xx responses. Register it
// with engine.Use before the routes, after gin.Recovery.
func Middleware(tt *totallytics.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		r, m := tt.Start(c.Request, "gin")
		if m == nil {
			c.Next()
			return
		}
		c.Request = r

		finished := false
		defer func() {
			if finished {
				return
			}
			p := recover()
			if p == nil || p == http.ErrAbortHandler {
				m.Finish(status(c, true), c.FullPath(), nil)
			} else {
				m.Finish(http.StatusInternalServerError, c.FullPath(), fmt.Errorf("panic: %v", p))
			}
			if p != nil {
				panic(p)
			}
		}()

		c.Next()
		finished = true
		m.Finish(status(c, false), c.FullPath(), lastError(c))
	}
}

// status is 499 when nothing was written because the client went away or the
// handler was aborted.
func status(c *gin.Context, aborted bool) int {
	if !c.Writer.Written() && (aborted || c.Request.Context().Err() != nil) {
		return statusClientClosedRequest
	}
	return c.Writer.Status()
}

func lastError(c *gin.Context) error {
	if last := c.Errors.Last(); last != nil {
		return last
	}
	return nil
}
