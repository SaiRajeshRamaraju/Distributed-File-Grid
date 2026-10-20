"""Exercise real server/client binaries in a temporary three-node grid."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import struct
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
        cls.logs = BUILD_DIR / "integration-logs" / ("unified" if USE_UNIFIED else "standalone")
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
        commands = {"head_server": "head-server", "cluster_server": "cluster-server"}
        command = ([str(BUILD_DIR / "dfg"), commands[binary], *args]
                   if USE_UNIFIED else [str(BUILD_DIR / binary), *args])
        process = subprocess.Popen(
            command, cwd=cls.root,
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

    def test_invalid_management_port_reports_error(self):
        result = subprocess.run(
            [str(BUILD_DIR / "dfg"), "add-server", "--host", "127.0.0.1",
             "--port", "invalid"], capture_output=True, text=True, timeout=5,
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("Error:", result.stderr)

    def test_existing_remote_file_cannot_be_overwritten_by_an_upload(self):
        source = self.root / "original.source"
        source.write_bytes(b"original")
        self.client("upload", source, "preserved.bin")
        source.write_bytes(b"replacement")
        result = self.client("upload", source, "preserved.bin", succeeds=False)
        self.assertIn("already exists", result.stderr)
        output = self.root / "preserved.download"
        self.client("download", "preserved.bin", output)
        self.assertEqual(output.read_bytes(), b"original")

    def test_concurrent_uploads_to_one_name_commit_only_one_file(self):
        processes = []
        payloads = [b"first upload", b"second upload"]
        for index, payload in enumerate(payloads):
            source = self.root / f"concurrent-{index}.source"
            source.write_bytes(payload)
            processes.append(subprocess.Popen(
                [str(BUILD_DIR / "client"), "upload", str(source), "concurrent.bin"],
                cwd=self.root, env=self.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            ))
        try:
            for process in processes:
                process.communicate(timeout=20)
            self.assertEqual(sorted(process.returncode for process in processes), [0, 1])
        finally:
            for process in processes:
                if process.poll() is None:
                    process.kill()
                    process.wait()
        output = self.root / "concurrent.download"
        self.client("download", "concurrent.bin", output)
        self.assertIn(output.read_bytes(), payloads)

    def test_delete_empty_and_replicated_files(self):
        for name, payload in [("delete-empty.bin", b""), ("delete-data.bin", b"delete me")]:
            with self.subTest(name=name):
                source = self.root / (name + ".source")
                source.write_bytes(payload)
                self.client("upload", source, name)
                if payload:
                    # A previous partial deletion must not make a retry fail.
                    with socket.create_connection(("127.0.0.1", self.node_ports[0][0]), timeout=5) as sock:
                        sock.sendall(f"DELETE_CHUNK {name}_chunk_0\n".encode())
                        self.assertEqual(sock.makefile("rb").readline(), b"SUCCESS\n")
                self.client("delete", name)
                self.assertNotIn(name, self.client("list").stdout)
                for index in range(1, 4):
                    self.assertEqual(list((self.root / f"node-{index}").glob(f"*{name}*.dat")), [])

    def test_fragmented_storage_control_command(self):
        with socket.create_connection(("127.0.0.1", self.node_ports[0][0]), timeout=5) as sock:
            time.sleep(0.1)
            sock.sendall(b"DELETE_")
            time.sleep(0.1)
            sock.sendall(b"CHUNK already-missing_chunk_0\n")
            self.assertEqual(sock.makefile("rb").readline(), b"SUCCESS\n")

    def test_invalid_uploads_are_rejected_without_stopping_head(self):
        digest = hashlib.sha256(b"x").hexdigest()
        cases = [
            f"UPLOAD invalid.bin nope 1 {digest}\n",
            f"UPLOAD invalid.bin -1 1 {digest}\n",
            f"UPLOAD invalid.bin 1 0 {digest}\n",
            f"UPLOAD invalid.bin 0 0 {digest}\n",
            f"UPLOAD ../invalid.bin 1 1 {digest}\n",
        ]
        for request in cases:
            with self.subTest(request=request):
                with socket.create_connection(("127.0.0.1", self.head_port), timeout=5) as sock:
                    sock.sendall(request.encode())
                    self.assertTrue(sock.makefile("rb").readline().startswith(b"ERROR:"))
                self.assertNotIn("invalid.bin", self.client("list").stdout)

    def test_upload_count_order_and_size_must_match(self):
        digest = hashlib.sha256(b"x").hexdigest()
        cases = [
            (2, 1, f"CHUNK 0 1 {digest}\nxEOF\n"),
            (1, 1, f"CHUNK 1 1 {digest}\nxEOF\n"),
            (1, 1, "EOF\n"),
            (1, 1, f"CHUNK 0 -1 {digest}\n"),
            (268435457, 1, f"CHUNK 0 268435457 {digest}\n"),
        ]
        for size, count, body in cases:
            with self.subTest(size=size, count=count, body=body):
                with socket.create_connection(("127.0.0.1", self.head_port), timeout=5) as sock:
                    sock.sendall(f"UPLOAD rejected.bin {size} {count} {digest}\n".encode())
                    with sock.makefile("rb") as stream:
                        self.assertEqual(stream.readline(), b"READY\n")
                        sock.sendall(body.encode())
                        self.assertTrue(stream.readline().startswith(b"ERROR:"))
                self.assertNotIn("rejected.bin", self.client("list").stdout)

    def test_oversized_binary_message_does_not_stop_storage(self):
        with socket.create_connection(("127.0.0.1", self.node_ports[0][1]), timeout=5) as sock:
            sock.sendall(struct.pack("!II", 1, 0xffffffff))
            self.assertEqual(sock.recv(1), b"")
        self.assertEqual(self.nodes[0].poll(), None)

    def test_control_api_ids_and_fragmented_http_body(self):
        servers = self.api("/api/v1/servers/cluster")["servers"]
        server_id = servers[0]["id"]
        # An existing route must parse the numeric ID after the slash.
        with urllib.request.urlopen(
            f"http://127.0.0.1:{self.control_port}/api/v1/servers/cluster/{server_id}/health", timeout=5
        ) as response:
            self.assertEqual(response.status, 200)
        body = b'{"host":"127.0.0.1","port":65432}'
        with socket.create_connection(("127.0.0.1", self.control_port), timeout=5) as sock:
            sock.sendall(("POST /api/v1/servers/cluster HTTP/1.1\r\nHost: localhost\r\n"
                          f"Content-Length: {len(body)}\r\n\r\n").encode() + body[:10])
            time.sleep(0.1)
            sock.sendall(body[10:])
            with sock.makefile("rb") as stream:
                response = stream.read()
        self.assertIn(b"201 Created", response)
        registered = self.api("/api/v1/servers/cluster")["servers"]
        added_id = next(node["id"] for node in registered if node["port"] == 65432)
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.control_port}/api/v1/servers/cluster/{added_id}", method="DELETE"
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            self.assertEqual(response.status, 200)

    def test_missing_file_download_fails(self):
        result = self.client("download", "does-not-exist.bin",
                             self.root / "missing.bin", succeeds=False)
        self.assertIn("no chunks found", (result.stdout + result.stderr).lower())

    def test_fragmented_head_request(self):
        with socket.create_connection(("127.0.0.1", self.head_port), timeout=5) as sock:
            sock.sendall(b"DOWN")
            time.sleep(0.1)
            sock.sendall(b"LOAD does-not-exist.bin\n")
            with sock.makefile("rb") as stream:
                self.assertTrue(stream.readline().startswith(b"ERROR:"))

    def test_upload_header_and_payload_in_same_write(self):
        payload = b"a coalesced TCP upload\x00\xff"
        digest = hashlib.sha256(payload).hexdigest()
        name = "coalesced.bin"
        header = f"UPLOAD {name} {len(payload)} 1 {digest}\n"
        chunk = f"CHUNK 0 {len(payload)} {digest}\n"
        with socket.create_connection(("127.0.0.1", self.head_port), timeout=10) as sock:
            sock.sendall(header.encode() + chunk.encode() + payload + b"EOF\n")
            with sock.makefile("rb") as stream:
                self.assertEqual(stream.readline(), b"READY\n")
                self.assertEqual(stream.readline(), b"SUCCESS\n")
        output = self.root / "coalesced.download"
        self.client("download", name, output)
        self.assertEqual(output.read_bytes(), payload)

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
    parser.add_argument("--unified", action="store_true")
    args, unittest_args = parser.parse_known_args()
    BUILD_DIR = args.build_dir.resolve()
    USE_UNIFIED = args.unified
    unittest.main(argv=[__file__, *unittest_args], verbosity=2)
