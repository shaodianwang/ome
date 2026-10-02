package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestOverrideWinsUntilItExpiresOrIsCleared(t *testing.T) {
	override := &rateOverride{}
	now := time.Unix(1000, 0)
	if _, ok := override.current(now); ok {
		t.Fatal("no override set, but one reads as in force")
	}
	override.apply(0.9, 30, now)
	if rate, ok := override.current(now.Add(29 * time.Second)); !ok || rate != 0.9 {
		t.Fatalf("inside the window: rate=%g ok=%t", rate, ok)
	}
	if _, ok := override.current(now.Add(31 * time.Second)); ok {
		t.Fatal("the bounded override did not expire")
	}
	override.apply(0.5, 0, now)
	if rate, ok := override.current(now.Add(24 * time.Hour)); !ok || rate != 0.5 {
		t.Fatalf("an unbounded override must stay: rate=%g ok=%t", rate, ok)
	}
	override.clear()
	if _, ok := override.current(now); ok {
		t.Fatal("clear left the override in force")
	}
}

func TestControlEndpointSetsReportsAndRejects(t *testing.T) {
	override := &rateOverride{}
	configured := 0.01
	effective := func() float64 {
		if rate, ok := override.current(time.Now()); ok {
			return rate
		}
		return configured
	}
	handler := controlHandler(override, configured, effective)
	get := func(query string) (int, string) {
		rec := httptest.NewRecorder()
		handler(rec, httptest.NewRequest(http.MethodGet, controlPath+query, nil))
		return rec.Code, rec.Body.String()
	}
	if code, body := get(""); code != http.StatusOK || !strings.Contains(body, "rate=0.01\n") || !strings.Contains(body, "override=false\n") {
		t.Fatalf("plain read: %d %q", code, body)
	}
	if code, body := get("?value=0.9&for=45"); code != http.StatusOK || !strings.Contains(body, "rate=0.9\n") ||
		!strings.Contains(body, "override=true\n") || !strings.Contains(body, "until=") {
		t.Fatalf("bounded set: %d %q", code, body)
	}
	if code, body := get("?clear=1"); code != http.StatusOK || !strings.Contains(body, "rate=0.01\n") || strings.Contains(body, "until=") {
		t.Fatalf("clear: %d %q", code, body)
	}
	if code, _ := get("?value=high"); code != http.StatusBadRequest {
		t.Fatalf("a non-numeric value must be rejected, got %d", code)
	}
	if code, _ := get("?value=0.9&for=soon"); code != http.StatusBadRequest {
		t.Fatalf("a non-numeric window must be rejected, got %d", code)
	}
	if code, body := get("?value=0.9"); code != http.StatusOK || !strings.Contains(body, "override=true\n") || strings.Contains(body, "until=") {
		t.Fatalf("unbounded set: %d %q", code, body)
	}
}
