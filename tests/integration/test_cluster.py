"""Exercise real server/client binaries in a temporary three-node grid."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import unittest
import urllib.error
import urllib.request


def free_ports(count):
    # Hold all reservations until every port has been selected.
    sockets = [socket.socket() for _ in range(count)]
    try:
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


class ClusterIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sandbox = tempfile.TemporaryDirectory(prefix="dfg-ci-")
        cls.addClassCleanup(cls.sandbox.cleanup)
        cls.root = Path(cls.sandbox.name)
        cls.logs = BUILD_DIR / "integration-logs"
        cls.logs.mkdir(parents=True, exist_ok=True)
        cls.processes = []
        cls.addClassCleanup(cls.stop_services)
        cls.head_port, cls.control_port, *node_ports = free_ports(11)
        cls.node_ports = [node_ports[i:i + 3] for i in range(0, 9, 3)]
        cls.env = os.environ.copy()
        # Ignore developer service overrides and use only this test's config.
        for key in list(cls.env):
            if key.startswith("DFG_") or key in {
                "SERVER_HOST", "SERVER_PORT", "HEAD_SERVER_HOST", "HEAD_SERVER_PORT",
                "HEAD_SERVER_CONTROL_PORT", "CONTROL_PORT", "ZK_HOSTS",
                "STORAGE_CHUNK_DIR", "STORAGE_REPLICATION_FACTOR",
                "HEALTH_CHECKER_HOST", "HEALTH_CHECKER_PORT",
            }:
                del cls.env[key]
        cls.env.update(DFG_METADATA_DB=str(cls.root / "metadata.db"),
                       DFG_HEAD_HOST="127.0.0.1", DFG_HEAD_PORT=str(cls.head_port))
        config = cls.root / "config"
        config.mkdir()
        (config / "head_server.json").write_text(json.dumps({
            "server": {"port": cls.head_port},
            "control_api": {"port": cls.control_port},
            "storage": {"replication_factor": 3},
            "cluster_servers": [],
        }))
        (config / "cluster_server.json").write_text(json.dumps({
            "head_server": {"host": "127.0.0.1", "port": cls.head_port,
                            "control_port": cls.control_port},
        }))
        cls.start_service("head", "head_server")
        cls.wait_until(lambda: cls.api("/api/v1/status"), "head control API")
        cls.wait_for_port(cls.head_port)
        cls.nodes = []
        for index, (port, transfer, public) in enumerate(cls.node_ports, 1):
            node_env = cls.env | {"DFG_STORAGE_CHUNK_DIR": str(cls.root / f"node-{index}")}
            process = cls.start_service(
                f"node-{index}", "cluster_server", "--server-id", str(index),
                "--ip", "127.0.0.1", "--port", str(port),
                "--transfer-port", str(transfer), "--public-port", str(public),
                env=node_env,
            )
            cls.nodes.append(process)
            cls.wait_for_port(transfer)
            cls.wait_for_port(public)
        cls.wait_until(lambda: cls.api("/api/v1/servers/cluster")["total"] == 3,
                       "three registered storage nodes")

    @classmethod
    def start_service(cls, label, binary, *args, env=None):
        logfile = (cls.logs / f"{label}.log").open("w")
        cls.addClassCleanup(logfile.close)
        process = subprocess.Popen(
            [str(BUILD_DIR / binary), *args], cwd=cls.root,
            env=env or cls.env, stdin=subprocess.DEVNULL,
            stdout=logfile, stderr=subprocess.STDOUT,
        )
        cls.processes.append(process)
        return process

    @classmethod
    def stop_services(cls):
        # Only terminate processes owned by this test; never use pkill.
        for process in reversed(cls.processes):
            if process.poll() is None:
                process.terminate()
        for process in cls.processes:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        # Background servers do not propagate their exit code to unittest.
        # Surface sanitizer reports even when they occur during shutdown.
        for log in cls.logs.glob("*.log"):
            contents = log.read_text(errors="replace")
            if any(marker in contents for marker in (
                "ERROR: AddressSanitizer", "ERROR: LeakSanitizer", "runtime error:",
            )):
                raise AssertionError(f"Sanitizer finding in {log}:\n{contents}")

    @classmethod
    def wait_until(cls, predicate, description):
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if any(process.poll() is not None for process in cls.processes):
                raise AssertionError(f"Service exited while waiting for {description}; see {cls.logs}")
            try:
                if predicate():
                    return
            except (OSError, urllib.error.URLError):
                pass
            time.sleep(0.05)
        raise AssertionError(f"Timed out waiting for {description}; see {cls.logs}")

    @classmethod
    def wait_for_port(cls, port):
        def connected():
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return True
        cls.wait_until(connected, f"TCP port {port}")

    @classmethod
    def api(cls, route):
        with urllib.request.urlopen(
            f"http://127.0.0.1:{cls.control_port}{route}", timeout=2
        ) as response:
            return json.load(response)

    def client(self, *args, succeeds=True):
        result = subprocess.run(
            [str(BUILD_DIR / "client"), *map(str, args)], cwd=self.root,
            env=self.env, capture_output=True, text=True, timeout=40,
        )
        with (self.logs / "client.log").open("a") as logfile:
            logfile.write(f"$ client {' '.join(map(str, args))}\n{result.stdout}{result.stderr}\n")
        if succeeds:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        return result

    def roundtrip(self, name, payload):
        source = self.root / (name + ".source")
        output = self.root / (name + ".download")
        source.write_bytes(payload)
        self.client("upload", source, name)
        self.client("download", name, output)
        self.assertEqual(output.read_bytes(), payload)
        self.assertEqual(hashlib.sha256(output.read_bytes()).digest(),
                         hashlib.sha256(payload).digest())
        self.assertIn(name, self.client("list").stdout)

    def test_binary_and_empty_file_roundtrips(self):
        for name, payload in [
            ("empty.bin", b""),
            ("binary.bin", bytes(range(256)) * 1024),
            ("large.bin", bytes(range(256)) * 8192),
        ]:
            with self.subTest(name=name):
                self.roundtrip(name, payload)

    def test_missing_file_download_fails(self):
        result = self.client("download", "does-not-exist.bin",
                             self.root / "missing.bin", succeeds=False)
        self.assertIn("no chunks found", (result.stdout + result.stderr).lower())

    def test_fragmented_public_chunk_request(self):
        payload = bytes(range(256)) * 16
        source = self.root / "fragmented.source"
        source.write_bytes(payload)
        self.client("upload", source, "fragmented.bin")
        public_port = self.node_ports[0][2]
        with socket.create_connection(("127.0.0.1", public_port), timeout=5) as sock:
            # Let the server accept before delivering any request bytes.
            time.sleep(0.1)
            sock.sendall(b"GET_CHUNK 0 ")
            time.sleep(0.1)
            sock.sendall(b"fragmented.bin\n")
            with sock.makefile("rb") as stream:
                self.assertEqual(stream.readline(), b"CHUNK_SIZE 4096\n")
                self.assertEqual(stream.read(len(payload)), payload)

    def test_z_download_survives_loss_of_one_replica(self):
        # Sort last: this test deliberately stops one of the three nodes.
        payload = bytes(range(256)) * 1024
        source = self.root / "replicated.source"
        source.write_bytes(payload)
        self.client("upload", source, "replicated.bin")
        for index in range(1, 4):
            chunks = list((self.root / f"node-{index}").glob("*replicated.bin*.dat"))
            self.assertEqual(len(chunks), 1, f"Missing replica on node {index}")
            self.assertEqual(chunks[0].read_bytes(), payload)
        self.nodes[0].terminate()
        self.nodes[0].wait(timeout=5)
        output = self.root / "replicated.download"
        self.client("download", "replicated.bin", output)
        self.assertEqual(output.read_bytes(), payload)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-dir", required=True, type=Path)
    args, unittest_args = parser.parse_known_args()
    BUILD_DIR = args.build_dir.resolve()
    unittest.main(argv=[__file__, *unittest_args], verbosity=2)
