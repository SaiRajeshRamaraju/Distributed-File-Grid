#!/usr/bin/env python3
"""Format owned C++ sources, or check them without changing files."""

import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    formatter = shutil.which(os.environ.get("CLANG_FORMAT", "clang-format"))
    if not formatter:
        parser.error("Install clang-format or set CLANG_FORMAT to its executable path")
    root = Path(__file__).resolve().parents[1]
    sources = sorted(str(path) for path in (root / "src").rglob("*")
                     if path.suffix in {".cpp", ".hpp"})
    flags = ["--dry-run", "--Werror"] if args.check else ["-i"]
    return subprocess.run([formatter, *flags, *sources], cwd=root, check=False).returncode


if __name__ == "__main__":
    sys.exit(main())
