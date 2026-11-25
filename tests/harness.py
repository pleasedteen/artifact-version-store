"""Shared plumbing for the test suite: a real server, a real client, fixtures."""

from __future__ import annotations

import hashlib
import http.client
import json
import os
import random
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import traceback
import urllib.parse
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Response:
    def __init__(self, status: int, headers: dict[str, str], body: bytes) -> None:
        self.status = status
        self.headers = {k.lower(): v for k, v in headers.items()}
        self.body = body

    def json(self) -> Any:
        return json.loads(self.body)

    def __repr__(self) -> str:
        preview = self.body[:200]
        return f"<Response {self.status} {preview!r}>"


class Client:
    """Tiny dependency-free HTTP client with keep-alive and streaming reads."""

    def __init__(self, base: str, timeout: float = 20.0) -> None:
        parts = urllib.parse.urlsplit(base)
        self.host = parts.hostname
        self.port = parts.port or 80
        self.timeout = timeout
        self._conn: Optional[http.client.HTTPConnection] = None

    def _connection(self) -> http.client.HTTPConnection:
        if self._conn is None:
            self._conn = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def request(
        self,
        method: str,
        path: str,
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> Response:
        hdrs = dict(headers or {})
        if body is not None:
            hdrs.setdefault("Content-Length", str(len(body)))
        for attempt in (0, 1):
            conn = self._connection()
            try:
                conn.request(method, path, body=body, headers=hdrs)
                raw = conn.getresponse()
                data = raw.read()
                return Response(raw.status, dict(raw.getheaders()), data)
            except (http.client.HTTPException, ConnectionError, OSError):
                conn.close()
                self._conn = None
                if attempt == 1:
                    raise
        raise AssertionError("unreachable")

    def post_json(self, path: str, payload: dict, headers: dict[str, str] | None = None) -> Response:
        hdrs = {"Content-Type": "application/json"}
        hdrs.update(headers or {})
        return self.request("POST", path, json.dumps(payload).encode(), hdrs)

    def get(self, path: str) -> Response:
        return self.request("GET", path)

    # -- convenience for the API under test ---------------------------------
    def create_upload(self, name: str, length: int) -> dict:
        resp = self.post_json("/v1/uploads", {"name": name, "length": length})
        assert resp.status == 201, resp
        return resp.json()

    def put_segment(self, upload_id: str, index: int, offset: int, data: bytes) -> Response:
        return self.request(
            "PUT",
            f"/v1/uploads/{upload_id}/segments/{index}?offset={offset}",
            data,
            {"X-Chunk-SHA256": hashlib.sha256(data).hexdigest()},
        )

    def complete(self, upload_id: str, key: Optional[str] = None) -> Response:
        headers = {"Idempotency-Key": key} if key else {}
        return self.request("POST", f"/v1/uploads/{upload_id}/complete", b"{}", headers)

    def download(self, name: str, version_id: Optional[str] = None) -> Response:
        path = f"/v1/objects/{quote(name)}/content"
        if version_id:
            path += f"/{version_id}"
        return self.get(path)

    def download_by_version(self, version_id: str) -> Response:
        return self.get(f"/v1/versions/{version_id}/content")

    def usage(self) -> dict:
        return self.get("/v1/usage").json()


def quote(name: str) -> str:
    return urllib.parse.quote(name, safe="/")


class StreamReader:
    """Reads a download body with byte-level control (to simulate a pause).

    A deliberately tiny receive buffer makes the server block on ``write`` once
    the reader stops consuming, so "paused" means the server really has the
    file pinned rather than the whole body already sitting in a socket buffer.
    """

    def __init__(
        self, base: str, path: str, timeout: float = 30.0, rcvbuf: int = 2048
    ) -> None:
        parts = urllib.parse.urlsplit(base)
        host, port = parts.hostname, parts.port or 80
        self.conn = http.client.HTTPConnection(host, port, timeout=timeout)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
        sock.settimeout(timeout)
        sock.connect((host, port))
        self.conn.sock = sock
        self.conn.request("GET", path)
        self.response = self.conn.getresponse()
        self.status = self.response.status
        self.headers = {k.lower(): v for k, v in self.response.getheaders()}
        self.received = 0

    @property
    def version_id(self) -> str:
        return self.headers.get("x-version-id", "")

    def read(self, n: int) -> bytes:
        data = self.response.read(n)
        self.received += len(data)
        return data

    def read_all(self, expected: Optional[int] = None) -> bytes:
        chunks = [self.response.read(65536)]
        while chunks[-1]:
            chunks.append(self.response.read(65536))
        data = b"".join(chunks)
        self.received += len(data)
        if expected is not None and len(data) != expected:
            raise AssertionError(f"short download: {len(data)} of {expected}")
        return data

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass


class ServerProcess:
    """Runs the service as a real subprocess so it can be SIGKILL-ed."""

    def __init__(
        self,
        data_dir: str,
        quota: int,
        port: Optional[int] = None,
        log: Optional[str] = None,
        env: Optional[dict[str, str]] = None,
    ) -> None:
        self.data_dir = data_dir
        self.quota = quota
        self.port = port or free_port()
        self.log_path = log or os.path.join(tempfile.gettempdir(), f"objstore-{self.port}.log")
        self.proc: Optional[subprocess.Popen] = None
        self.base = f"http://127.0.0.1:{self.port}"
        self.env = dict(env or {})

    def start(self) -> "ServerProcess":
        log = open(self.log_path, "ab")
        env = dict(os.environ)
        env.update(self.env)
        self.proc = subprocess.Popen(
            [
                sys.executable, "-X", "utf8", "-m", "objstore.serve",
                "--port", str(self.port),
                "--data-dir", self.data_dir,
                "--quota", str(self.quota),
                "--quiet",
            ],
            cwd=ROOT,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        deadline = time.time() + 20
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"server exited early; see {self.log_path}")
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=0.25):
                    return self
            except OSError:
                time.sleep(0.02)
        raise RuntimeError(f"server did not start; see {self.log_path}")

    def kill(self, sig: int = 9) -> None:
        if self.proc is None:
            return
        self.proc.send_signal(sig)
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=5)
        self.proc = None

    def client(self, timeout: float = 20.0) -> Client:
        return Client(self.base, timeout=timeout)

    def __enter__(self) -> "ServerProcess":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.kill()


@dataclass
class Check:
    name: str
    ok: Optional[bool] = None
    detail: str = ""
    duration: float = 0.0


@dataclass
class Result:
    checks: list[Check] = field(default_factory=list)

    def add(self, name: str, ok: bool, detail: str = "", duration: float = 0.0) -> Check:
        check = Check(name, ok, detail, duration)
        self.checks.append(check)
        return check

    @property
    def passed(self) -> int:
        return sum(1 for c in self.checks if c.ok)

    @property
    def failed(self) -> list[Check]:
        return [c for c in self.checks if not c.ok]


class Runner:
    """Runs named scenario functions, each against fresh server state."""

    def __init__(self, verbose: bool = True) -> None:
        self.verbose = verbose
        self.result = Result()

    def check(self, name: str, fn: Callable[[], Any], detail: str = "") -> None:
        start = time.time()
        try:
            value = fn()
            text = detail or (str(value) if value is not None else "")
            self.result.add(name, True, text, time.time() - start)
            if self.verbose:
                print(f"  \033[32mPASS\033[0m {name}" + (f"  ({text})" if text else ""), flush=True)
        except Exception as err:  # noqa: BLE001
            self.result.add(name, False, f"{type(err).__name__}: {err}", time.time() - start)
            if self.verbose:
                print(f"  \033[31mFAIL\033[0m {name}: {type(err).__name__}: {err}", flush=True)
                traceback.print_exc()


class TempDir:
    def __enter__(self) -> str:
        self.path = tempfile.mkdtemp(prefix="objstore-test-")
        return self.path

    def __exit__(self, *exc) -> None:
        shutil.rmtree(self.path, ignore_errors=True)


def random_bytes(n: int, seed: Optional[int] = None) -> bytes:
    rng = random.Random(seed) if seed is not None else random.Random()
    return bytes(rng.getrandbits(8) for _ in range(n))
