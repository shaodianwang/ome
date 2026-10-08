# hack/

## Code agents and sandboxes

OME builds work out-of-the-box on a fully-provisioned developer Mac, but several steps fail subtly in stripped-down code-agent containers. Workarounds:

- **System packages** (Debian/Ubuntu base images): `apt-get install -y jq libssl-dev pkg-config build-essential curl ca-certificates`.
  - `jq` is consumed by `make manifests` (CRD post-processing in `hack/` scripts).
  - `libssl-dev pkg-config build-essential` are required to build `pkg/xet` (Rust → static C lib).
- **Rust toolchain** for `pkg/xet`:
  ```sh
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --default-toolchain stable --profile minimal
  ```
- **`make generate` writes to the Go env file** (`~/.config/go/env`) if you run it on a Makefile from before commit `[Misc] Makefile: avoid go env -w in make generate`. Update or rebase before reporting failures.
- **IPv6-only CI sandboxes**: envtest's `kube-apiserver` auto-detects the pod's IPv6 external address, then refuses to start because it doesn't match the IPv4 service-cluster range. Wrap the binary with a shim pinning `--advertise-address=127.0.0.1`; see `.buildkite/run_tests.sh` for the canonical pattern.
- **envtest debugging**: `export KUBEBUILDER_ATTACH_CONTROL_PLANE_OUTPUT=true` to forward kube-apiserver/etcd output into test logs.
- **xet-linked binaries / tests**: `export LD_LIBRARY_PATH=$(pwd)/pkg/xet/target/release` so the dynamic linker finds `libxet.{so,dylib}` (needed for `ome-agent` tests and any binary that links the Rust staticlib path).
- **KUBEBUILDER_ASSETS one-liner**: rather than retyping the full `bin/setup-envtest` invocation for every `go test` call, save this as a shell alias / persistent env:
  ```sh
  export KUBEBUILDER_ASSETS="$(bin/setup-envtest use "$(cat .envtest-k8s-version)" -p path)"
  ```
