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
//	FAIL_AT=after-Ns       run normally for N seconds, then exit 1
//	                       (delayed crash, soak-test the recovery path)
//	FAIL_ON_NODE=<node>    return 503 to /healthz only while running on
//	                       <node> — a node-scoped fault: the identical
//	                       workload is healthy on any other node. The
//	                       pod spec supplies NODE_NAME via a fieldRef.
//	FAIL_ON_POD=<pod>      narrows FAIL_ON_NODE to one pod name (the
//	                       pod spec supplies POD_NAME via a fieldRef);
//	                       other pods on the failing node stay healthy.
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

	if life, ok := parseAfter(failAt); ok {
		go func() {
			time.Sleep(life)
			fmt.Printf("FAIL_AT=%s timer fired, exiting 1\n", failAt)
			os.Exit(1)
		}()
	}

	nodeFault := nodeScopedFault()

	// EXIT_AFTER_READY_SEC stops the process that many seconds after it
	// starts serving, exiting 0: the shape of a runner that serves and
	// then stops itself, which the kubelet restarts at once. It counts
	// from the moment the listener is up, which is when a pod with no
	// readiness probe reads Ready and within a probe period of it
	// otherwise.
	if d, err := strconv.Atoi(os.Getenv("EXIT_AFTER_READY_SEC")); err == nil && d > 0 {
		exitAfterReady := time.Duration(d) * time.Second
		go func() {
			time.Sleep(exitAfterReady)
			fmt.Printf("EXIT_AFTER_READY_SEC=%s elapsed since serving began, exiting 0\n", exitAfterReady)
			os.Exit(0)
		}()
	}

	http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if failAt == "ready" {
			http.Error(w, "FAIL_AT=ready", http.StatusServiceUnavailable)
			return
		}
		if nodeFault {
			http.Error(w, "FAIL_ON_NODE="+os.Getenv("FAIL_ON_NODE"), http.StatusServiceUnavailable)
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

// nodeScopedFault reports whether this process should fail readiness
// because it landed on the FAIL_ON_NODE node (optionally narrowed to
// the FAIL_ON_POD pod). NODE_NAME / POD_NAME come from pod-spec
// fieldRefs; unset FAIL_ON_NODE means no node-scoped fault.
func nodeScopedFault() bool {
	failNode := os.Getenv("FAIL_ON_NODE")
	if failNode == "" || failNode != os.Getenv("NODE_NAME") {
		return false
	}
	failPod := os.Getenv("FAIL_ON_POD")
	return failPod == "" || failPod == os.Getenv("POD_NAME")
}

// parseAfter extracts the run's life from an "after-Nm" (minutes) or
// "after-Ns" (seconds) FAIL_AT value. Returns (0, false) when the value
// matches neither shape.
func parseAfter(failAt string) (time.Duration, bool) {
	rest, ok := strings.CutPrefix(failAt, "after-")
	if !ok || len(rest) < 2 {
		return 0, false
	}
	unit := time.Minute
	switch rest[len(rest)-1] {
	case 'm':
	case 's':
		unit = time.Second
	default:
		return 0, false
	}
	n, err := strconv.Atoi(rest[:len(rest)-1])
	if err != nil || n <= 0 {
		return 0, false
	}
	return time.Duration(n) * unit, true
}
