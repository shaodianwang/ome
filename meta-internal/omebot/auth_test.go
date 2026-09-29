//go:build meta_internal

package omebot

import (
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

func TestVerifierHMAC(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	v := &Verifier{Mode: AuthModeHMAC, Secret: []byte("s3cret"), MaxSkew: 5 * time.Minute, Now: func() time.Time { return now }}
	body := []byte(`{"text":"help"}`)
	sign := func(ts int64, secret string, b []byte) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/v1/command", nil)
		r.Header.Set(HeaderTimestamp, strconv.FormatInt(ts, 10))
		r.Header.Set(HeaderSignature, Sign([]byte(secret), ts, b))
		rec := httptest.NewRecorder()
		if err := v.Verify(r, body); err != nil {
			rec.WriteHeader(401)
		} else {
			rec.WriteHeader(200)
		}
		return rec
	}
	if rec := sign(now.Unix(), "s3cret", body); rec.Code != 200 {
		t.Fatalf("valid signature rejected")
	}
	if rec := sign(now.Unix(), "wrong", body); rec.Code != 401 {
		t.Fatalf("wrong secret accepted")
	}
	if rec := sign(now.Unix(), "s3cret", []byte(`{"text":"other"}`)); rec.Code != 401 {
		t.Fatalf("tampered body accepted")
	}
	if rec := sign(now.Add(-6*time.Minute).Unix(), "s3cret", body); rec.Code != 401 {
		t.Fatalf("stale timestamp accepted")
	}
	r := httptest.NewRequest("POST", "/v1/command", nil)
	if err := v.Verify(r, body); err == nil {
		t.Fatal("unsigned request accepted")
	}
	none := &Verifier{Mode: AuthModeNone}
	if err := none.Verify(r, body); err != nil {
		t.Fatalf("mode none must accept: %v", err)
	}
}
