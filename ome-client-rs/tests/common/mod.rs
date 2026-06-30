//! envtest-reuse harness for the OME Rust client integration tests.
//!
//! Boots the `etcd` + `kube-apiserver` binaries from `KUBEBUILDER_ASSETS` (the
//! same ones OME's Go suite uses — set it with
//! `KUBEBUILDER_ASSETS="$(bin/setup-envtest use 1.30 -p path)"`), installs the
//! OME CRDs via the bundled `kubectl`, and returns a connected `kube::Client`.
//! No Docker; same tier as `make integration-test`. Processes are killed on drop.

#![allow(dead_code)]

use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use kube::config::{Config, KubeConfigOptions, Kubeconfig};
use kube::Client;

pub struct TestCluster {
    pub client: Client,
    pub kubeconfig: PathBuf,
    pub kubectl: PathBuf,
    _apiserver: ChildGuard,
    _etcd: ChildGuard,
    _dir: tempfile::TempDir,
}

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Allocate `n` distinct free TCP ports. Binding every listener *before* reading
/// any port guarantees they differ (a bind-then-close-in-a-loop can hand back
/// the same ephemeral port twice); they're released when this returns, so a
/// small TOCTOU window remains before the child binds — fine for a test harness.
fn free_ports(n: usize) -> Vec<u16> {
    let listeners: Vec<TcpListener> = (0..n)
        .map(|_| TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port"))
        .collect();
    listeners
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect()
}

fn assets() -> Result<PathBuf> {
    std::env::var("KUBEBUILDER_ASSETS")
        .map(PathBuf::from)
        .context("KUBEBUILDER_ASSETS not set (run via `make rust-integration-test`)")
}

/// Boot etcd + kube-apiserver, install the OME CRDs, return a connected cluster.
pub async fn start() -> Result<TestCluster> {
    let assets = assets()?;
    let dir = tempfile::tempdir()?;
    let dp = dir.path();

    // One self-signed keypair: apiserver serving cert + service-account keys.
    let pki = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into(), "localhost".into()])?;
    let cert_pem = dp.join("serving.crt");
    let key_pem = dp.join("serving.key");
    let sa_pub = dp.join("sa.pub");
    std::fs::write(&cert_pem, pki.cert.pem())?;
    std::fs::write(&key_pem, pki.signing_key.serialize_pem())?;
    std::fs::write(&sa_pub, pki.signing_key.public_key_pem())?;

    // Static bearer token: AlwaysAllow disables anonymous auth, so the client
    // must authenticate as *some* user. token-auth-file maps this token to a
    // user (AlwaysAllow then authorizes it). Format: token,user,uid,groups.
    let token = "envtest-token";
    let token_file = dp.join("tokens.csv");
    std::fs::write(
        &token_file,
        format!("{token},envtest-admin,envtest-admin,system:masters\n"),
    )?;

    let [etcd_port, etcd_peer_port, api_port] = {
        let p = free_ports(3);
        [p[0], p[1], p[2]]
    };
    let _etcd = ChildGuard(
        Command::new(assets.join("etcd"))
            .arg("--data-dir")
            .arg(dp.join("etcd"))
            .arg("--listen-client-urls")
            .arg(format!("http://127.0.0.1:{etcd_port}"))
            .arg("--advertise-client-urls")
            .arg(format!("http://127.0.0.1:{etcd_port}"))
            .arg("--listen-peer-urls")
            .arg(format!("http://127.0.0.1:{etcd_peer_port}"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .context("spawn etcd")?,
    );

    // Capture apiserver stderr to a file so a startup failure is diagnosable
    // (surfaced in the readiness-timeout error below) rather than swallowed.
    let api_log = dp.join("apiserver.log");
    let _apiserver = ChildGuard(
        Command::new(assets.join("kube-apiserver"))
            .args(["--etcd-servers", &format!("http://127.0.0.1:{etcd_port}")])
            .args(["--secure-port", &api_port.to_string()])
            .arg("--tls-cert-file")
            .arg(&cert_pem)
            .arg("--tls-private-key-file")
            .arg(&key_pem)
            .arg("--service-account-key-file")
            .arg(&sa_pub)
            .arg("--service-account-signing-key-file")
            .arg(&key_pem)
            .args(["--service-account-issuer", "https://kubernetes.default.svc"])
            .args(["--api-audiences", "https://kubernetes.default.svc"])
            .args(["--authorization-mode", "AlwaysAllow"])
            .arg("--token-auth-file")
            .arg(&token_file)
            .args(["--disable-admission-plugins", "ServiceAccount"])
            // The apiserver listens on 127.0.0.1 via --bind-address (default
            // 0.0.0.0); --advertise-address only sets the built-in `kubernetes`
            // Service endpoint, which k8s rejects if it's a loopback IP — and
            // that gates /readyz. A non-loopback documentation IP keeps the
            // bootstrap hook happy; our client dials 127.0.0.1:port directly and
            // never touches that Service.
            .args(["--advertise-address", "192.0.2.1"])
            .arg("--allow-privileged")
            .stdout(Stdio::null())
            .stderr(Stdio::from(std::fs::File::create(&api_log)?))
            .spawn()
            .context("spawn kube-apiserver")?,
    );

    let server = format!("https://127.0.0.1:{api_port}");
    let kubeconfig = dp.join("kubeconfig");
    std::fs::write(
        &kubeconfig,
        format!(
            "apiVersion: v1\nkind: Config\nclusters:\n- name: envtest\n  cluster:\n    server: {server}\n    insecure-skip-tls-verify: true\nusers:\n- name: envtest\n  user:\n    token: {token}\ncontexts:\n- name: envtest\n  context:\n    cluster: envtest\n    user: envtest\ncurrent-context: envtest\n"
        ),
    )?;

    let kubectl = assets.join("kubectl");

    // Wait for the apiserver to report ready.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let out = Command::new(&kubectl)
            .args(["--kubeconfig", kubeconfig.to_str().unwrap()])
            .args(["get", "--raw=/readyz"])
            .output();
        if let Ok(o) = out {
            if o.status.success() && String::from_utf8_lossy(&o.stdout).contains("ok") {
                break;
            }
        }
        if Instant::now() > deadline {
            let log = std::fs::read_to_string(&api_log).unwrap_or_default();
            let tail: Vec<&str> = log.lines().rev().take(20).collect();
            let tail = tail.into_iter().rev().collect::<Vec<_>>().join("\n");
            bail!(
                "kube-apiserver did not become ready within 60s.\napiserver stderr (tail):\n{tail}"
            );
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    // Install the OME CRDs (the same YAML the Go client is generated from).
    // Apply each ome.io_*.yaml explicitly — the directory also holds a
    // kustomization.yaml that kubectl would choke on. Use server-side apply: the
    // big CRDs (inlined pod-template schema) exceed the 256KB annotation limit
    // that client-side apply's last-applied-configuration would impose.
    let crd_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../config/crd/full");
    let mut crd_files: Vec<PathBuf> = std::fs::read_dir(&crd_dir)
        .context("read config/crd/full")?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("ome.io_") && n.ends_with(".yaml"))
        })
        .collect();
    crd_files.sort();
    if crd_files.is_empty() {
        bail!("no ome.io_*.yaml CRDs found in {}", crd_dir.display());
    }
    let mut cmd = Command::new(&kubectl);
    cmd.args(["--kubeconfig", kubeconfig.to_str().unwrap()])
        .args(["apply", "--server-side", "--force-conflicts"]);
    for f in &crd_files {
        cmd.arg("-f").arg(f);
    }
    let apply = cmd.output().context("kubectl apply CRDs")?;
    if !apply.status.success() {
        bail!(
            "kubectl apply CRDs failed: {}",
            String::from_utf8_lossy(&apply.stderr)
        );
    }
    let _ = Command::new(&kubectl)
        .args(["--kubeconfig", kubeconfig.to_str().unwrap()])
        .args([
            "wait",
            "--for=condition=Established",
            "crd",
            "--all",
            "--timeout=60s",
        ])
        .output();

    // Build the kube client straight from the kubeconfig file — no process-env
    // mutation, so parallel tests never clobber each other's KUBECONFIG. Force no
    // proxy: the apiserver is local, and a corporate HTTPS_PROXY would otherwise
    // trip kube's disabled http-proxy feature.
    let kc = Kubeconfig::read_from(&kubeconfig).context("read kubeconfig")?;
    let mut config = Config::from_custom_kubeconfig(kc, &KubeConfigOptions::default())
        .await
        .context("kube Config from kubeconfig")?;
    config.proxy_url = None;
    config.accept_invalid_certs = true;
    let client = Client::try_from(config).context("kube::Client from envtest kubeconfig")?;

    Ok(TestCluster {
        client,
        kubeconfig,
        kubectl,
        _apiserver,
        _etcd,
        _dir: dir,
    })
}
