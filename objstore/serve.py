"""Entrypoint: ``python -m objstore.serve`` starts the local HTTP service."""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading

from .http_api import serve
from .storage import Storage


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="objstore",
        description="Single-node, versioned, quota-governed local object store.",
    )
    parser.add_argument("--host", default="127.0.0.1", help="bind address (default 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8080, help="TCP port (default 8080)")
    parser.add_argument(
        "--data-dir",
        default=os.environ.get("OBJSTORE_DIR", os.path.abspath("objstore-data")),
        help="directory for the database and content (default ./objstore-data)",
    )
    parser.add_argument(
        "--quota",
        default=os.environ.get("OBJSTORE_QUOTA", str(64 * 1024 * 1024)),
        help="content byte quota, e.g. 64M or an integer (default 64 MiB)",
    )
    parser.add_argument("--quiet", action="store_true", help="suppress per-request logs")
    parser.add_argument(
        "--print-config",
        action="store_true",
        help="print the resolved configuration as JSON and exit",
    )
    args = parser.parse_args(argv)

    try:
        quota = parse_size(args.quota)
    except ValueError as err:
        parser.error(str(err))

    config = {
        "host": args.host,
        "port": args.port,
        "data_dir": os.path.abspath(args.data_dir),
        "quota_bytes": quota,
    }
    if args.print_config:
        print(json.dumps(config))
        return 0

    storage = Storage(config["data_dir"], quota)
    server = serve(args.host, args.port, storage, quiet=args.quiet)
    stop = threading.Event()

    def _shutdown(signum, frame):  # noqa: ARG001
        stop.set()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    print(
        json.dumps({"event": "listening", **config, "port": server.server_address[1]}),
        flush=True,
    )
    thread = threading.Thread(target=server.serve_forever, name="http", daemon=True)
    thread.start()
    try:
        while not stop.wait(0.2):
            pass
    finally:
        server.shutdown()
        server.server_close()
        storage.close()
    return 0


def parse_size(value: str) -> int:
    """Accept ``1048576`` or ``64M``/``512K``/``2G`` (decimal-free, 1024-based)."""
    text = str(value).strip().lower().replace("_", "")
    if not text:
        raise ValueError("quota must not be empty")
    units = {"kb": 1024, "mb": 1024**2, "gb": 1024**3, "k": 1024, "m": 1024**2, "g": 1024**3, "b": 1}
    factor = 1
    number = text
    for unit, mult in units.items():
        if text.endswith(unit) and len(text) > len(unit):
            number, factor = text[: -len(unit)], mult
            break
    try:
        amount = int(number)
    except ValueError:
        raise ValueError(f"invalid size: {value!r}") from None
    if amount < 0:
        raise ValueError("quota must be >= 0")
    return amount * factor


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
