"""Check public CLI entry points without starting services."""

import argparse
from pathlib import Path
import subprocess
import unittest


class CliSmokeTest(unittest.TestCase):
    def test_help_and_version_for_every_entry_point(self):
        commands = [[name] for name in (
            "dfg", "client", "head_server", "cluster_server",
            "health_checker", "zk_head_server_monitor",
        )]
        commands += [["dfg", service] for service in (
            "head-server", "cluster-server", "health-checker", "zk-monitor",
        )]
        for command in commands:
            for flag in ("--help", "--version"):
                with self.subTest(command=command, flag=flag):
                    result = subprocess.run(
                        [str(BUILD_DIR / command[0]), *command[1:], flag],
                        capture_output=True, text=True, timeout=5,
                    )
                    self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                    self.assertTrue(result.stdout.strip())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-dir", required=True, type=Path)
    args, unittest_args = parser.parse_known_args()
    BUILD_DIR = args.build_dir.resolve()
    unittest.main(argv=[__file__, *unittest_args], verbosity=2)
