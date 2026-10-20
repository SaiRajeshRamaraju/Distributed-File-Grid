"""Exercise untrusted download responses without a real grid."""

import argparse
from contextlib import contextmanager
import hashlib
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import unittest


@contextmanager
def peer(response):
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        listener.settimeout(5)
        errors = []

        def respond():
            try:
                connection, _ = listener.accept()
                with connection:
                    connection.settimeout(5)
                    with connection.makefile("rb") as request:
                        request.readline()
                        connection.sendall(response)
            except (BrokenPipeError, ConnectionResetError):
                pass
            except Exception as error:
                errors.append(error)

        worker = threading.Thread(target=respond)
        worker.start()
        try:
            yield listener.getsockname()[1]
        finally:
            worker.join(timeout=6)
            if worker.is_alive():
                raise AssertionError("Test peer did not finish")
            if errors:
                raise errors[0]


class ClientFailureTest(unittest.TestCase):
    def attempt(self, metadata, directory, output):
        with peer(metadata) as port:
            result = subprocess.run(
                [str(BUILD_DIR / "client"), "download", "test.bin", str(output),
                 "--server_ip", "localhost", "--server_port", str(port)],
                capture_output=True, text=True, timeout=10,
            )
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertEqual(list(Path(directory).glob("*.dfg-*")), [])

    def test_rejects_incomplete_or_duplicate_download_metadata(self):
        digest = hashlib.sha256(b"x").hexdigest()
        cases = [b"EOF\n", f"FILE_HASH {digest}\nCHUNKS 1\nEOF\n".encode(),
                 (f"FILE_HASH {digest}\nCHUNKS 2\n"
                  f"CHUNK_LOC 0 localhost 1 {digest}\n"
                  f"CHUNK_LOC 0 localhost 1 {digest}\nEOF\n").encode()]
        for metadata in cases:
            with self.subTest(metadata=metadata), tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "saved.bin"
                output.write_bytes(b"original")
                self.attempt(metadata, directory, output)
                self.assertEqual(output.read_bytes(), b"original")

    def test_failed_integrity_check_preserves_existing_output(self):
        payload = b"payload"
        digest = hashlib.sha256(payload).hexdigest()
        invalid_digest = hashlib.sha256(b"different file").hexdigest()
        for chunk_response, file_digest in [
            (b"CHUNK_SIZE 999999999999\n", digest),
            (b"CHUNK_SIZE -1\n", digest),
            (b"CHUNK_SIZE 7\nwrong!!", digest),
            (b"CHUNK_SIZE 7\n" + payload, invalid_digest),
        ]:
            with self.subTest(response=chunk_response), tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "saved.bin"
                output.write_bytes(b"original")
                with peer(chunk_response) as port:
                    metadata = (f"FILE_HASH {file_digest}\nCHUNKS 1\n"
                                f"CHUNK_LOC 0 localhost {port} {digest}\nEOF\n").encode()
                    self.attempt(metadata, directory, output)
                self.assertEqual(output.read_bytes(), b"original")

    def test_cannot_replace_a_directory_with_downloaded_data(self):
        with tempfile.TemporaryDirectory() as directory:
            digest = hashlib.sha256(b"").hexdigest()
            self.attempt(f"FILE_HASH {digest}\nEOF\n".encode(), directory, Path(directory))
            self.assertTrue(Path(directory).is_dir())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-dir", required=True, type=Path)
    args, unittest_args = parser.parse_known_args()
    BUILD_DIR = args.build_dir.resolve()
    unittest.main(argv=[__file__, *unittest_args], verbosity=2)
