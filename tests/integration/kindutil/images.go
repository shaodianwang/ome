package kindutil

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// ImageBuildSpec declares a docker-build-and-kind-load action that
// KindSuiteOpts wires into suite startup. ContextDir is relative to
// the repo root; Tag is the local image reference (e.g.
// "fake-serving:v1") that pod specs use.
type ImageBuildSpec struct {
	// ContextDir is the docker build context directory, relative to
	// the repo root. Must contain a Dockerfile.
	ContextDir string

	// Tag is the local image tag the build produces. Pod specs use
	// this exact string as image:. No registry prefix.
	Tag string

	// ExtraTags lists additional `docker tag` aliases pointing at the
	// same built image. Useful when two image references should map to
	// the same binary — e.g. two tags used purely as a ControllerRevision
	// trigger for revision-hash drift tests where runtime behavior is
	// identical. NOT suitable for in-place-update tests: kubelet
	// short-circuits the digest equality check when two aliases resolve
	// to the same sha256, leaving Status.ContainerStatuses[*].Image at
	// the original alias name and stalling
	// `ops.podRuntimeImagesMatch`. Use BuildArgs + two separate
	// ImageBuildSpec entries when the test needs a true content change.
	ExtraTags []string

	// BuildArgs threads `--build-arg KEY=VAL` into the docker build.
	// Two entries sharing the same ContextDir but distinct BuildArgs
	// produce distinct image digests, which is what in-place update
	// tests need so kubelet's runtime-image equality check resolves
	// the post-bump container as different from the pre-bump one.
	BuildArgs map[string]string
}

// buildCache memoizes per-process which content-hashes we've already
// built. Reusing the same KindSuiteOpts across multiple suites in the
// same test binary doesn't redo the work.
var (
	buildCacheMu sync.Mutex
	buildCache   = make(map[string]string) // ContextDir → built hash
)

// BuildAndLoadImages applies every spec in order: docker-build, tag,
// then `kind load docker-image` into clusterName. Builds are skipped
// when the context's content hash matches the last build for that
// ContextDir within this test process.
//
// Returns the first error encountered; partial progress is left in
// place (the user can re-run with the offending spec excluded).
func BuildAndLoadImages(ctx context.Context, clusterName string, specs []ImageBuildSpec) error {
	for _, spec := range specs {
		if err := buildAndLoad(ctx, clusterName, spec); err != nil {
			return fmt.Errorf("BuildAndLoadImages: %s: %w", spec.Tag, err)
		}
	}
	return nil
}

// buildAndLoad runs the build + tag + load for one spec. Content-hash
// caching skips the docker-build when nothing has changed; the kind
// load still runs because cluster-side state isn't cached here.
//
// ContextDir is interpreted relative to the OME repo root so test
// authors can spell the path the way it appears in `git status`,
// regardless of where `go test` is invoked from.
func buildAndLoad(ctx context.Context, clusterName string, spec ImageBuildSpec) error {
	if spec.ContextDir == "" {
		return fmt.Errorf("ContextDir is empty")
	}
	if spec.Tag == "" {
		return fmt.Errorf("Tag is empty")
	}

	absCtx, err := resolveContextDir(spec.ContextDir)
	if err != nil {
		return fmt.Errorf("resolve %s: %w", spec.ContextDir, err)
	}

	hash, err := contextHash(absCtx)
	if err != nil {
		return fmt.Errorf("hash %s: %w", absCtx, err)
	}
	// Fold BuildArgs into the cache key so two specs over the same
	// ContextDir that differ only by --build-arg both build (otherwise
	// the second skip-because-cached and resolves to the first's
	// image content, which is exactly what we're trying to avoid for
	// in-place update tests).
	cacheKey := absCtx + "|" + buildArgsCacheKey(spec.BuildArgs)

	buildCacheMu.Lock()
	cached, hit := buildCache[cacheKey]
	buildCacheMu.Unlock()

	if !hit || cached != hash {
		if err := dockerBuild(ctx, absCtx, spec.Tag, spec.BuildArgs); err != nil {
			return err
		}
		buildCacheMu.Lock()
		buildCache[cacheKey] = hash
		buildCacheMu.Unlock()
	}

	for _, alias := range spec.ExtraTags {
		if err := dockerTag(ctx, spec.Tag, alias); err != nil {
			return err
		}
	}

	loadTags := append([]string{spec.Tag}, spec.ExtraTags...)
	return kindLoad(ctx, clusterName, loadTags)
}

// buildArgsCacheKey emits a deterministic flattened form of buildArgs
// so two specs with the same args (in any iteration order) hash to the
// same cache key.
func buildArgsCacheKey(args map[string]string) string {
	if len(args) == 0 {
		return ""
	}
	keys := make([]string, 0, len(args))
	for k := range args {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+args[k])
	}
	return strings.Join(parts, ",")
}

// dockerBuild runs `docker build -t <tag> [--build-arg KEY=VAL ...] <contextDir>`.
// Defaults to DOCKER_BUILDKIT=0 (the legacy builder) — fake-serving's
// Dockerfile is plain multi-stage with no BuildKit-only syntax, and forcing
// buildx would require users to install the buildx component, which not
// every Docker install ships with (notably colima + docker.io upstream).
// A caller-set DOCKER_BUILDKIT wins: daemons running the containerd image
// store (common in CI dind setups) reject the legacy builder, so CI exports
// DOCKER_BUILDKIT=1.
func dockerBuild(ctx context.Context, contextDir, tag string, buildArgs map[string]string) error {
	args := []string{"build", "-t", tag}
	keys := make([]string, 0, len(buildArgs))
	for k := range buildArgs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		args = append(args, "--build-arg", k+"="+buildArgs[k])
	}
	args = append(args, contextDir)
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Env = os.Environ()
	if os.Getenv("DOCKER_BUILDKIT") == "" {
		cmd.Env = append(cmd.Env, "DOCKER_BUILDKIT=0")
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker build -t %s %s: %w (stderr: %s)",
			tag, contextDir, err, stderr.String())
	}
	return nil
}

// dockerTag runs `docker tag <src> <dst>`.
func dockerTag(ctx context.Context, src, dst string) error {
	cmd := exec.CommandContext(ctx, "docker", "tag", src, dst)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker tag %s %s: %w (stderr: %s)",
			src, dst, err, stderr.String())
	}
	return nil
}

// kindLoad runs `kind load docker-image <tag>... --name <cluster>`.
// Loading multiple tags at once is faster than one-per-call because
// each invocation pays the cluster-state lookup overhead.
func kindLoad(ctx context.Context, clusterName string, tags []string) error {
	args := append([]string{"load", "docker-image"}, tags...)
	args = append(args, "--name", clusterName)
	cmd := exec.CommandContext(ctx, "kind", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("kind load docker-image %s --name %s: %w (stderr: %s)",
			strings.Join(tags, " "), clusterName, err, stderr.String())
	}
	return nil
}

// contextHash returns a stable SHA-256 hex digest of every regular
// file under contextDir, plus the relative paths. Drives the
// build-skip optimization; collisions are vanishingly unlikely for
// the small test contexts we hash.
//
// Dotfiles and directories aren't excluded — fake-serving's context
// is small enough that the cost is negligible. If callers add larger
// contexts they should rely on docker's own build cache instead.
func contextHash(contextDir string) (string, error) {
	type fileEntry struct {
		rel  string
		size int64
		mod  int64
	}
	var entries []fileEntry
	err := filepath.WalkDir(contextDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(contextDir, path)
		if err != nil {
			return err
		}
		entries = append(entries, fileEntry{rel: rel, size: info.Size(), mod: info.ModTime().UnixNano()})
		return nil
	})
	if err != nil {
		return "", err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].rel < entries[j].rel })

	h := sha256.New()
	for _, e := range entries {
		fmt.Fprintf(h, "%s\x00%d\x00%d\n", e.rel, e.size, e.mod)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// resolveContextDir interprets ContextDir as either an absolute path
// or a path relative to the OME repo root, returning an absolute path.
// Detection: repo root is the nearest ancestor with a go.mod whose
// module line matches github.com/ome-projects/ome.
//
// Used by BuildAndLoadImages so tests can spell ContextDir relative
// to the repo (the natural copy-paste path) regardless of where
// `go test` is invoked.
func resolveContextDir(rel string) (string, error) {
	if filepath.IsAbs(rel) {
		return rel, nil
	}
	root, err := repoRoot()
	if err != nil {
		return "", err
	}
	return filepath.Join(root, rel), nil
}

// repoRoot walks up from CWD until it finds the OME go.mod.
func repoRoot() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	dir := cwd
	for {
		if data, err := os.ReadFile(filepath.Join(dir, "go.mod")); err == nil {
			if bytes.Contains(data, []byte("module github.com/ome-projects/ome")) {
				return dir, nil
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("repoRoot: walked up from %s without finding OME go.mod", cwd)
		}
		dir = parent
	}
}
