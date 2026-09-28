// Package totallyticsecho records requests to an echo instance for
// Totallytics API analytics, with echo routes as templates.
//
//	e := echo.New()
//	e.Use(middleware.Recover(), totallyticsecho.Middleware(tt))
//	e.GET("/users/:id", getUser)
package totallyticsecho

import (
	"fmt"
	"net/http"

	"github.com/bitgate/totallytics-go"
	"github.com/labstack/echo/v4"
)

const statusClientClosedRequest = 499

// Middleware records every request the instance handles, including 404s and
// panics, which it records as 500 and re-panics for middleware.Recover.
// A returned error goes through the echo error handler right away, so the
// status the client gets is recorded along with the error, and is then
// returned unchanged. Register it with e.Use, after middleware.Recover.
func Middleware(tt *totallytics.Client) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			r, m := tt.Start(c.Request(), "echo")
			if m == nil {
				return next(c)
			}
			c.SetRequest(r)

			finished := false
			defer func() {
				if finished {
					return
				}
				p := recover()
				if p == nil || p == http.ErrAbortHandler {
					m.Finish(status(c, true), c.Path(), nil)
				} else {
					m.Finish(http.StatusInternalServerError, c.Path(), fmt.Errorf("panic: %v", p))
				}
				if p != nil {
					panic(p)
				}
			}()

			err := next(c)
			if err != nil {
				c.Error(err)
			}
			finished = true
			m.Finish(status(c, false), c.Path(), err)
			return err
		}
	}
}

// status is 499 when nothing was written because the client went away or the
// handler was aborted.
func status(c echo.Context, aborted bool) int {
	response := c.Response()
	if !response.Committed && (aborted || c.Request().Context().Err() != nil) {
		return statusClientClosedRequest
	}
	return response.Status
}
