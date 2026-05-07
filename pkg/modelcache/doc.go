// Package modelcache defines OME's control-plane port for cache-backed model
// weight distribution.
//
// The package intentionally contains no transport implementation. Concrete
// adapters may call a daemon API, an FFI binding, or another mechanism, but
// OME controllers should depend on the semantic operations defined here.
package modelcache
