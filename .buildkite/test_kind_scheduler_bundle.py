"""Exercise the offline scheduler image boundary with real bundle files."""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent.parent


class SchedulerBundleTest(unittest.TestCase):
    def test_only_verified_binary_reaches_docker(self):
        cases = (
            ("valid", {}, False, "", True),
            ("wrong commit", {"commit": "other"}, False, "commit mismatch", False),
            ("not ready", {"state": "pending"}, False, "not ready", False),
            ("wrong OS", {"goos": "darwin"}, False, "platform mismatch", False),
            ("wrong architecture", {"goarch": "arm64"}, False, "platform mismatch", False),
            ("missing checksum", {"scheduler_sha256": ""}, False, "no checksum", False),
            ("corrupt binary", {"scheduler_sha256": "0" * 64}, False, "FAILED", False),
            ("missing binary", {}, True, "No such file", False),
        )
        for name, overrides, remove_binary, error, succeeds in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temp:
                stage = Path(temp) / "bundle with spaces"
                (stage / "scheduler").mkdir(parents=True)
                binary = stage / "scheduler/ome-scheduler"
                binary.write_bytes(b"test scheduler")
                fields = {
                    "state": "ready", "commit": "expected", "goos": "linux", "goarch": "amd64",
                    "scheduler_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                    **overrides,
                }
                (stage / "manifest").write_text("".join(f"{k}={v}\n" for k, v in fields.items()))
                if remove_binary:
                    binary.unlink()
                docker = Path(temp) / "docker"
                docker.write_text(
                    "#!/usr/bin/env python3\nimport json, os, sys\n"
                    "with open(os.environ['DOCKER_CALL'], 'w') as out:\n"
                    "    json.dump([os.environ.get('DOCKER_BUILDKIT'), sys.argv[1:]], out)\n"
                )
                docker.chmod(0o755)
                call = Path(temp) / "docker-call.json"
                result = subprocess.run(
                    ["sh", str(ROOT / ".buildkite/build_kind_scheduler.sh"), str(stage)],
                    cwd=ROOT, text=True, capture_output=True, check=False,
                    env={**os.environ, "PATH": temp + os.pathsep + os.environ["PATH"],
                         "BUILDKITE_COMMIT": "expected", "DOCKER_CALL": str(call)},
                )
                self.assertEqual(result.returncode == 0, succeeds, result.stdout + result.stderr)
                self.assertEqual(call.exists(), succeeds)
                if succeeds:
                    self.assertEqual(json.loads(call.read_text()), ["0", [
                        "build", "--pull=false", "-f", "dockerfiles/kind-prebuilt.Dockerfile",
                        "--build-arg", "BASE_IMAGE=scratch", "--build-arg", "RUN_USER=65532:65532",
                        "-t", "ome-scheduler:kind-local", str(stage / "scheduler"),
                    ]])
                    self.assertEqual(binary.stat().st_mode & 0o777, 0o755)
                    self.assertEqual((stage / "scheduler/entrypoint").resolve(), binary)
                else:
                    self.assertIn(error, result.stdout + result.stderr)
