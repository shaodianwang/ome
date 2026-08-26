package kindutil

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// LoadImage makes an image already present on the host's Docker daemon
// available to a KIND cluster's containerd.
//
// `kind load docker-image` is the fast path and the one CI takes. It fails on
// hosts whose Docker uses the containerd image store (Docker Desktop and
// Colima both default to it now): kind exports the image and imports it with
// `ctr --all-platforms`, but a multi-platform image pulled on one architecture
// only has that platform's blobs locally, so the import aborts with
// "content digest ... not found".
//
// The fallback does the same job without the flag — save the image, copy the
// archive into the node, and import just the platform that is actually there.
// Kept as a fallback rather than the default because the direct path is faster
// and is what every other suite relies on.
func LoadImage(ctx context.Context, clusterName, tag string) error {
	if err := kindLoad(ctx, clusterName, []string{tag}); err == nil {
		return nil
	}
	return loadImageViaNodeImport(ctx, clusterName, tag)
}

// loadImageViaNodeImport saves tag to a temporary archive, copies it into every
// node of the cluster, and imports it with ctr directly.
func loadImageViaNodeImport(ctx context.Context, clusterName, tag string) error {
	archive := filepath.Join(os.TempDir(), fmt.Sprintf("kindload-%s.tar", sanitizeTag(tag)))
	defer func() { _ = os.Remove(archive) }()

	if out, err := exec.CommandContext(ctx, "docker", "save", tag, "-o", archive).CombinedOutput(); err != nil {
		return fmt.Errorf("LoadImage: docker save %s: %w (%s)", tag, err, out)
	}

	nodes, err := clusterNodes(ctx, clusterName)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		dest := "/" + filepath.Base(archive)
		if out, err := exec.CommandContext(ctx, "docker", "cp", archive, node+":"+dest).CombinedOutput(); err != nil {
			return fmt.Errorf("LoadImage: docker cp to %s: %w (%s)", node, err, out)
		}
		importCmd := exec.CommandContext(ctx, "docker", "exec", node,
			"ctr", "--namespace=k8s.io", "images", "import", "--digests",
			"--snapshotter=overlayfs", dest)
		if out, err := importCmd.CombinedOutput(); err != nil {
			return fmt.Errorf("LoadImage: ctr import on %s: %w (%s)", node, err, out)
		}
		_ = exec.CommandContext(ctx, "docker", "exec", node, "rm", "-f", dest).Run()
	}
	return nil
}

// clusterNodes lists the container names backing a KIND cluster.
func clusterNodes(ctx context.Context, clusterName string) ([]string, error) {
	cmd := exec.CommandContext(ctx, "kind", "get", "nodes", "--name", clusterName)
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("kind get nodes --name %s: %w (stderr: %s)", clusterName, err, stderr.String())
	}
	var nodes []string
	for _, line := range splitLines(out.String()) {
		if line != "" {
			nodes = append(nodes, line)
		}
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("kind cluster %s has no nodes", clusterName)
	}
	return nodes, nil
}

// sanitizeTag turns an image reference into something usable as a filename.
func sanitizeTag(tag string) string {
	out := []rune(tag)
	for i, r := range out {
		switch r {
		case '/', ':', '@':
			out[i] = '-'
		}
	}
	return string(out)
}

// splitLines splits on newlines without pulling in strings.Split's empty-tail
// behavior at call sites.
func splitLines(s string) []string {
	var lines []string
	start := 0
	for i, r := range s {
		if r == '\n' {
			lines = append(lines, trimCR(s[start:i]))
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, trimCR(s[start:]))
	}
	return lines
}

func trimCR(s string) string {
	if len(s) > 0 && s[len(s)-1] == '\r' {
		return s[:len(s)-1]
	}
	return s
}
