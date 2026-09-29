//go:build meta_internal

package omebot

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strconv"
	"time"
)

const (
	// HeaderTimestamp carries the Unix seconds the request was signed at.
	HeaderTimestamp = "X-OME-Bot-Timestamp"
	// HeaderSignature carries hex(HMAC-SHA256(secret, timestamp + "\n" + body)).
	HeaderSignature = "X-OME-Bot-Signature"
)

// Verifier authenticates HTTP callers of the service.
type Verifier struct {
	Mode    string
	Secret  []byte
	MaxSkew time.Duration
	Now     func() time.Time
}

// Sign produces the signature header value for a body at a timestamp.
func Sign(secret []byte, timestamp int64, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(strconv.FormatInt(timestamp, 10)))
	mac.Write([]byte("\n"))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// Verify checks the request headers against the body already read.
func (v *Verifier) Verify(r *http.Request, body []byte) error {
	if v.Mode == AuthModeNone {
		return nil
	}
	tsRaw := r.Header.Get(HeaderTimestamp)
	sig := r.Header.Get(HeaderSignature)
	if tsRaw == "" || sig == "" {
		return errors.New("missing signature headers")
	}
	ts, err := strconv.ParseInt(tsRaw, 10, 64)
	if err != nil {
		return errors.New("malformed timestamp")
	}
	now := time.Now()
	if v.Now != nil {
		now = v.Now()
	}
	if d := now.Sub(time.Unix(ts, 0)); d > v.MaxSkew || d < -v.MaxSkew {
		return errors.New("request timestamp outside the accepted window")
	}
	want := Sign(v.Secret, ts, body)
	if !hmac.Equal([]byte(want), []byte(sig)) {
		return errors.New("signature mismatch")
	}
	return nil
}
