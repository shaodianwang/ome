package kindutil

import (
	"reflect"
	"strings"
	"testing"
)

func TestDockerBuildArgv(t *testing.T) {
	got := dockerBuildArgv("dockerfiles", "alfred.Dockerfile", "alfred:kind",
		map[string]string{"ALFRED_OSS_REF": "abc", "A": "1"})
	want := []string{"build", "-t", "alfred:kind",
		"--build-arg", "A=1", "--build-arg", "ALFRED_OSS_REF=abc",
		"-f", "dockerfiles/alfred.Dockerfile", "dockerfiles"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("argv = %v, want %v", got, want)
	}
}

func TestDockerBuildArgvDefaultDockerfile(t *testing.T) {
	got := dockerBuildArgv("tests/integration/kindutil/fake-serving", "", "fake-serving:v1", nil)
	want := []string{"build", "-t", "fake-serving:v1", "tests/integration/kindutil/fake-serving"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("argv = %v, want %v", got, want)
	}
}

func TestDockerBuildEnv(t *testing.T) {
	t.Setenv("DOCKER_BUILDKIT", "")
	if got := dockerBuildEnv(false); got != "DOCKER_BUILDKIT=0" {
		t.Fatalf("legacy default: got %q", got)
	}
	if got := dockerBuildEnv(true); got != "DOCKER_BUILDKIT=1" {
		t.Fatalf("RequireBuildKit: got %q", got)
	}
	t.Setenv("DOCKER_BUILDKIT", "0")
	if got := dockerBuildEnv(true); got != "DOCKER_BUILDKIT=1" {
		t.Fatalf("RequireBuildKit must override a caller-set 0: got %q", got)
	}
	if got := dockerBuildEnv(false); got != "" {
		t.Fatalf("caller-set 0 wins when not required: got %q", got)
	}
	t.Setenv("DOCKER_BUILDKIT", "1")
	if got := dockerBuildEnv(false); got != "" {
		t.Fatalf("caller-set value wins when not required: got %q", got)
	}
}

func TestDockerBuildEnvironOverride(t *testing.T) {
	t.Setenv("DOCKER_BUILDKIT", "0")
	env := dockerBuildEnviron([]string{"PATH=/usr/bin", "DOCKER_BUILDKIT=0"}, true)
	last := ""
	for _, kv := range env {
		if strings.HasPrefix(kv, "DOCKER_BUILDKIT=") {
			last = kv
		}
	}
	if last != "DOCKER_BUILDKIT=1" {
		t.Fatalf("last DOCKER_BUILDKIT entry = %q, want DOCKER_BUILDKIT=1 (exec keeps the last duplicate)", last)
	}
	if got := dockerBuildEnviron([]string{"DOCKER_BUILDKIT=0"}, false); len(got) != 1 {
		t.Fatalf("caller-set value must be left alone when BuildKit is not required: %v", got)
	}
}
