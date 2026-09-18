#!/usr/bin/env python3
"""Compile and run the bundled Thrift regression test using CXX and CXXFLAGS."""

import os
from pathlib import Path
import shlex
import subprocess
import tarfile
import tempfile
import unittest


class BundledThriftTest(unittest.TestCase):
    def test_enum_iterator(self):
        crate_dir = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as temp_dir:
            with tarfile.open(crate_dir / "duckdb.tar.gz") as archive:
                members = [
                    member
                    for member in archive.getmembers()
                    if member.name.startswith("duckdb/third_party/thrift/")
                ]
                archive.extractall(temp_dir, members=members, filter="data")

            executable = Path(temp_dir) / "tenum_iterator"
            for standard in ("c++11", "c++17", "c++20"):
                with self.subTest(standard=standard):
                    subprocess.run(
                        [
                            *shlex.split(os.environ.get("CXX", "c++")),
                            *shlex.split(os.environ.get("CXXFLAGS", "")),
                            f"-std={standard}",
                            "-UNDEBUG",
                            "-I",
                            str(Path(temp_dir) / "duckdb/third_party/thrift"),
                            str(crate_dir / "tests/tenum_iterator.cpp"),
                            "-o",
                            str(executable),
                        ],
                        check=True,
                    )
                    subprocess.run([str(executable)], check=True, timeout=30)


if __name__ == "__main__":
    unittest.main()
