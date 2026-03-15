#Environment check

from __future__ import annotations

import sys
from importlib.metadata import PackageNotFoundError, version


REQUIRED_MAJOR = 3
REQUIRED_MINOR = 12

PACKAGES = [
    "kafka-python",
    "pandas",
    "pyarrow",
    "pyflink",
    "psycopg2-binary",
]


def package_version(name: str) -> str:
    try:
        return version(name)
    except PackageNotFoundError:
        return "not installed"


def main() -> None:
    py_version = sys.version_info
    print("Environment check")
    print(f"- Python: {py_version.major}.{py_version.minor}.{py_version.micro}")
    if (py_version.major, py_version.minor) != (REQUIRED_MAJOR, REQUIRED_MINOR):
        print(
            f"  Warning: expected Python {REQUIRED_MAJOR}.{REQUIRED_MINOR} "
            "from pyproject.toml"
        )

    print("- Key dependencies:")
    for name in PACKAGES:
        print(f"  - {name}: {package_version(name)}")


if __name__ == "__main__":
    main()
