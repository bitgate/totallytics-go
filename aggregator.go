package totallytics

import "math"

const (
	maxKeys               = 10_000
	maxServerErrorSamples = 50
	maxClientErrorSamples = 20
)

// measurement is one finished request, validated and clipped.
type measurement struct {
	ts         int64
	method     string
	route      string
	path       string
	status     int
	durationMs float64
	userAgent  string
	consumer   string
	message    string
}

type rowKey struct {
	minute    int64
	method    string
	route     string
	status    int
	userAgent string
	consumer  string
}

type metricRow struct {
	Minute        int64         `json:"minute"`
	Method        string        `json:"method"`
	Route         string        `json:"route"`
	Status        int           `json:"status"`
	Count         int64         `json:"count"`
	DurationMsSum float64       `json:"duration_ms_sum"`
	Histogram     map[int]int64 `json:"histogram"`
	UserAgent     string        `json:"user_agent,omitempty"`
	Consumer      string        `json:"consumer,omitempty"`
}

type errorRow struct {
	TS         int64   `json:"ts"`
	Method     string  `json:"method"`
	Route      string  `json:"route"`
	Path       string  `json:"path"`
	Status     int     `json:"status"`
	DurationMs float64 `json:"duration_ms"`
	UserAgent  string  `json:"user_agent,omitempty"`
	Consumer   string  `json:"consumer,omitempty"`
	Message    string  `json:"message,omitempty"`
}

// aggregator sums measurements per minute, method, route, status, user agent
// and consumer, keeping rows in first-seen order.
type aggregator struct {
	index        map[rowKey]*metricRow
	rows         []*metricRow
	serverErrors []errorRow
	clientErrors []errorRow
}

func (a *aggregator) size() int {
	return len(a.rows)
}

// add reports false when m would open a row beyond maxKeys.
func (a *aggregator) add(m measurement) bool {
	key := rowKey{
		minute:    m.ts / 60_000 * 60,
		method:    m.method,
		route:     m.route,
		status:    m.status,
		userAgent: m.userAgent,
		consumer:  m.consumer,
	}

	row := a.index[key]
	if row == nil {
		if len(a.rows) >= maxKeys {
			return false
		}
		if a.index == nil {
			a.index = make(map[rowKey]*metricRow)
		}
		row = &metricRow{
			Minute:    key.minute,
			Method:    m.method,
			Route:     m.route,
			Status:    m.status,
			Histogram: make(map[int]int64),
			UserAgent: m.userAgent,
			Consumer:  m.consumer,
		}
		a.index[key] = row
		a.rows = append(a.rows, row)
	}

	row.Count++
	row.DurationMsSum += m.durationMs
	row.Histogram[bucket(m.durationMs)]++

	if m.status >= 400 {
		a.sample(m)
	}
	return true
}

// drain returns the rows and error samples, server errors first, and resets the aggregator.
func (a *aggregator) drain() ([]metricRow, []errorRow) {
	metrics := make([]metricRow, len(a.rows))
	for i, row := range a.rows {
		metrics[i] = *row
		metrics[i].DurationMsSum = round3(row.DurationMsSum)
	}
	samples := append(a.serverErrors, a.clientErrors...)

	*a = aggregator{}
	return metrics, samples
}

func (a *aggregator) sample(m measurement) {
	samples, limit := &a.clientErrors, maxClientErrorSamples
	if m.status >= 500 {
		samples, limit = &a.serverErrors, maxServerErrorSamples
	}
	if len(*samples) >= limit {
		return
	}

	*samples = append(*samples, errorRow{
		TS:         m.ts,
		Method:     m.method,
		Route:      m.route,
		Path:       m.path,
		Status:     m.status,
		DurationMs: round3(m.durationMs),
		UserAgent:  m.userAgent,
		Consumer:   m.consumer,
		Message:    m.message,
	})
}

func round3(ms float64) float64 {
	return math.Round(ms*1000) / 1000
}
