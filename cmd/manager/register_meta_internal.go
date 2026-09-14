//go:build meta_internal

package main

// Deployment-specific extensions are compiled in only under the meta_internal
// build tag. A default build omits this file, so it never references the
// optional tree and keeps building after that tree is deleted.
import _ "github.com/ome-projects/ome/meta-internal/capacityformat"
