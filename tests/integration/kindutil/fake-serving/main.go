// Command fake-serving is the tiny pod-image OME's KIND-backed
// integration tests use for behavior-style coverage. It does just
// enough to look like a real inference runtime to OMENative's
// ContainersReady aggregation: bind :8080, answer /healthz with 200.
//
// Failure modes are env-var driven so a test can fixture them
// declaratively:
//
//	FAIL_AT=startup        exit 1 immediately (CrashLoopBackOff)
//	FAIL_AT=ready          listen on :8080 but return 503 to /healthz
//	                       (kubelet sees liveness/readiness fail)
//	FAIL_AT=after-Nm       run normally for N minutes, then exit 1
//	                       (delayed crash, soak-test the recovery path)
//	STARTUP_DELAY_SEC=N    sleep N seconds before binding (slow start)
//	(unset)                normal pod
//
// The OMENative serving-gate (ome.io/serving=True label/condition) is
// flipped by the controller separately — fake-serving doesn't look at
// it. Pod readiness == ContainersReady AND ome.io/serving=True, where
// ContainersReady is what this binary controls.
package main

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const listenAddr = ":8080"

func main() {
	failAt := os.Getenv("FAIL_AT")

	if failAt == "startup" {
		fmt.Println("FAIL_AT=startup, exiting 1")
		os.Exit(1)
	}

	if d, err := strconv.Atoi(os.Getenv("STARTUP_DELAY_SEC")); err == nil && d > 0 {
		fmt.Printf("STARTUP_DELAY_SEC=%d, sleeping before bind\n", d)
		time.Sleep(time.Duration(d) * time.Second)
	}

	if mins, ok := parseAfterMinutes(failAt); ok {
		go func() {
			time.Sleep(time.Duration(mins) * time.Minute)
			fmt.Printf("FAIL_AT=after-%dm timer fired, exiting 1\n", mins)
			os.Exit(1)
		}()
	}

	http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if failAt == "ready" {
			http.Error(w, "FAIL_AT=ready", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ok")
	})

	fmt.Printf("fake-serving listening on %s (FAIL_AT=%q)\n", listenAddr, failAt)
	if err := http.ListenAndServe(listenAddr, nil); err != nil {
		fmt.Printf("listen error: %v\n", err)
		os.Exit(1)
	}
}

// parseAfterMinutes extracts N from a "after-Nm" FAIL_AT value.
// Returns (0, false) when the value doesn't match the expected shape.
func parseAfterMinutes(failAt string) (int, bool) {
	if !strings.HasPrefix(failAt, "after-") || !strings.HasSuffix(failAt, "m") {
		return 0, false
	}
	mins, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(failAt, "after-"), "m"))
	if err != nil || mins <= 0 {
		return 0, false
	}
	return mins, true
}
