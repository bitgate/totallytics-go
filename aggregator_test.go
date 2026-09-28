package totallytics

import (
	"fmt"
	"testing"
	"time"
)

var minute = time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC).UnixMilli()

func request(route string, status int, ms float64) measurement {
	return measurement{ts: minute, method: "GET", route: route, path: route, status: status, durationMs: ms}
}

func TestAggregatorMergesPerMinuteAndKey(t *testing.T) {
	var a aggregator
	first := request("/users/:id", 200, 12)
	late := first
	late.ts = minute + 59_999
	late.durationMs = 30
	nextMinute := first
	nextMinute.ts = minute + 60_000
	withConsumer := first
	withConsumer.consumer = "acme"
	for _, m := range []measurement{first, late, nextMinute, withConsumer} {
		if !a.add(m) {
			t.Fatalf("add(%+v) = false", m)
		}
	}

	metrics, samples := a.drain()
	if len(metrics) != 3 || len(samples) != 0 {
		t.Fatalf("got %d rows and %d samples, want 3 and 0", len(metrics), len(samples))
	}
	merged := metrics[0]
	if merged.Minute != minute/1000 || merged.Count != 2 || merged.DurationMsSum != 42 || merged.Histogram[bucket(12)] != 1 || merged.Histogram[bucket(30)] != 1 {
		t.Errorf("merged row = %+v", merged)
	}
	if metrics[1].Minute != minute/1000+60 {
		t.Errorf("next minute row has minute %d", metrics[1].Minute)
	}
	if metrics[2].Consumer != "acme" || metrics[2].Count != 1 {
		t.Errorf("consumer row = %+v", metrics[2])
	}
	if a.size() != 0 {
		t.Errorf("size after drain = %d", a.size())
	}
}

func TestAggregatorCapsKeys(t *testing.T) {
	var a aggregator
	for i := range maxKeys {
		if !a.add(request(fmt.Sprintf("/r/%d", i), 200, 1)) {
			t.Fatalf("row %d rejected", i)
		}
	}
	if a.add(request("/one-too-many", 200, 1)) {
		t.Error("accepted a row beyond maxKeys")
	}
	if !a.add(request("/r/0", 200, 1)) {
		t.Error("rejected an existing key at the cap")
	}
}

func TestAggregatorSamplesErrors(t *testing.T) {
	var a aggregator
	for i := range 90 {
		status := 500 + i%4
		if i%3 == 0 {
			status = 404
		}
		m := request(fmt.Sprintf("/e/%d", i), status, 5)
		m.message = fmt.Sprintf("failure %d", i)
		a.add(m)
	}
	a.add(request("/fine", 200, 5))

	_, samples := a.drain()
	if len(samples) != maxServerErrorSamples+maxClientErrorSamples {
		t.Fatalf("got %d samples, want %d", len(samples), maxServerErrorSamples+maxClientErrorSamples)
	}
	for i, s := range samples {
		serverError := s.Status >= 500
		if serverError != (i < maxServerErrorSamples) {
			t.Fatalf("sample %d has status %d, server errors must come first", i, s.Status)
		}
	}
	if samples[0].Message != "failure 1" || samples[maxServerErrorSamples].Message != "failure 0" {
		t.Errorf("samples out of order: %q, %q", samples[0].Message, samples[maxServerErrorSamples].Message)
	}
}

func TestDrainRoundsDurations(t *testing.T) {
	var a aggregator
	a.add(request("/sum", 200, 0.1))
	a.add(request("/sum", 200, 0.2))
	a.add(request("/fail", 500, 1.23456))

	metrics, samples := a.drain()
	if metrics[0].DurationMsSum != 0.3 {
		t.Errorf("duration_ms_sum = %v, want 0.3", metrics[0].DurationMsSum)
	}
	if samples[0].DurationMs != 1.235 {
		t.Errorf("error duration_ms = %v, want 1.235", samples[0].DurationMs)
	}
}

func TestMeasureClipsLikeJavaScript(t *testing.T) {
	long := func(prefix int, tail string) string {
		b := make([]byte, prefix)
		for i := range b {
			b[i] = 'a'
		}
		return string(b) + tail
	}
	cases := []struct {
		in    string
		limit int
		want  string
	}{
		{"short", 10, "short"},
		{long(511, "😀b"), 512, long(511, "")},
		{long(510, "😀b"), 512, long(510, "😀")},
		{"ééé", 2, "éé"},
	}
	for _, tc := range cases {
		if got := clip(tc.in, tc.limit); got != tc.want {
			t.Errorf("clip(%d bytes, %d) kept %d bytes, want %d", len(tc.in), tc.limit, len(got), len(tc.want))
		}
	}

	if _, ok := measure(entry{status: 99}); ok {
		t.Error("status 99 accepted")
	}
	if _, ok := measure(entry{status: 600}); ok {
		t.Error("status 600 accepted")
	}
	m, _ := measure(entry{method: "post", status: 200, durationMs: -3})
	if m.method != "POST" || m.path != "/" || m.route != "/" || m.durationMs != 0 {
		t.Errorf("measure defaults = %+v", m)
	}
}
