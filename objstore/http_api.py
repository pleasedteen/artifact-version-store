"""HTTP layer: a small, deliberately boring REST surface over :class:`Storage`."""

from __future__ import annotations

import hashlib
import json
import re
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional
from urllib.parse import parse_qs, quote, unquote, urlsplit

from .errors import ApiError
from .storage import ContentUnavailable, Storage

MAX_JSON_BODY = 1024 * 1024
CHUNK = 65536

_PATH_UPLOAD = re.compile(r"^/v1/uploads/([A-Za-z0-9_\-]+)$")
_PATH_UPLOAD_SEGMENT = re.compile(r"^/v1/uploads/([A-Za-z0-9_\-]+)/segments/(\d+)$")
_PATH_UPLOAD_COMPLETE = re.compile(r"^/v1/uploads/([A-Za-z0-9_\-]+)/complete$")
# Object names may contain slashes (build/path artifacts); they are matched
# greedily up to the fixed suffix.
_PATH_VERSIONS = re.compile(r"^/v1/objects/(.+)/versions$")
_PATH_VERSION = re.compile(r"^/v1/objects/(.+)/versions/([A-Za-z0-9_\-]+)$")
_PATH_DOWNLOAD = re.compile(r"^/v1/objects/(.+)/content$")
_PATH_DOWNLOAD_VERSION = re.compile(r"^/v1/objects/(.+)/content/([A-Za-z0-9_\-]+)$")
_PATH_VERSION_BY_ID = re.compile(r"^/v1/versions/([A-Za-z0-9_\-]+)$")
_PATH_VERSION_CONTENT = re.compile(r"^/v1/versions/([A-Za-z0-9_\-]+)/content$")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "objstore/1.0"
    sys_version = ""
    timeout = 60  # idle keep-alive connections release their worker thread

    # -- plumbing -----------------------------------------------------------
    @property
    def storage(self) -> Storage:
        return self.server.storage  # type: ignore[attr-defined]

    def log_message(self, fmt: str, *args) -> None:  # noqa: A003 - stdlib name
        if getattr(self.server, "quiet", True):
            return
        super().log_message(fmt, *args)

    def _read_body(self, limit: Optional[int]) -> bytes:
        self._body_consumed = False
        raw = self.headers.get("Content-Length")
        if raw is None:
            raise ApiError(411, "LengthRequired", "Content-Length is required")
        try:
            length = int(raw)
        except ValueError:
            raise ApiError(400, "BadRequest", "invalid Content-Length") from None
        if length < 0:
            raise ApiError(400, "BadRequest", "invalid Content-Length")
        if limit is not None and length > limit:
            raise ApiError(413, "TooLarge", f"body exceeds {limit} bytes")
        chunks: list[bytes] = []
        remaining = length
        if length:
            self._body_consumed = True
        while remaining:
            chunk = self.rfile.read(min(remaining, CHUNK))
            if not chunk:
                raise ApiError(400, "BadRequest", "request body truncated")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def _json_body(self) -> dict:
        if self.headers.get("Content-Length") is None:
            return {}
        raw = self._read_body(MAX_JSON_BODY)
        if not raw:
            return {}
        try:
            value = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise ApiError(400, "BadRequest", "body must be valid JSON") from None
        if not isinstance(value, dict):
            raise ApiError(400, "BadRequest", "body must be a JSON object")
        return value

    def _send_json(self, status: int, payload: object, headers: Optional[dict] = None) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode()
        self._respond_started = True
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _send_error(self, err: ApiError) -> None:
        self._send_json(err.status, err.payload(), err.headers)

    def _maybe_close_after_unread_body(self) -> None:
        """Never try to parse the rest of a connection after an early error.

        If the request declared a body that was not consumed, those bytes are
        still queued on the socket; keeping the connection alive would make the
        next request start mid-body.
        """
        declared = self.headers.get("Content-Length")
        if declared is None:
            return
        try:
            pending = int(declared) > 0
        except ValueError:
            pending = True
        if pending and not self._body_consumed:
            self.close_connection = True

    def _path(self) -> str:
        return unquote(urlsplit(self.path).path)

    def _query(self) -> dict[str, list[str]]:
        return parse_qs(urlsplit(self.path).query)

    def _dispatch(self, method: str) -> None:
        self._respond_started = False
        self._body_consumed = False
        try:
            self._route(method)
        except ApiError as err:
            if self._respond_started:
                self.close_connection = True
            else:
                self._send_error(err)
            self._maybe_close_after_unread_body()
        except ContentUnavailable:
            # Headers are already on the wire (this only happens mid-stream);
            # the only honest signal left is a truncated body + closed socket.
            self.close_connection = True
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True
        except ValueError as err:
            if not self._respond_started:
                self._send_error(ApiError(400, "BadRequest", str(err)))
                self._maybe_close_after_unread_body()
            else:
                self.close_connection = True
        except Exception:  # pragma: no cover - defensive
            traceback.print_exc()
            self.close_connection = True
            if not self._respond_started:
                self._send_error(ApiError(500, "Internal", "internal server error"))

    # -- verbs --------------------------------------------------------------
    def do_GET(self) -> None:  # noqa: N802 - stdlib name
        self._dispatch("GET")

    def do_HEAD(self) -> None:  # noqa: N802
        self._dispatch("HEAD")

    def do_POST(self) -> None:  # noqa: N802
        self._dispatch("POST")

    def do_PUT(self) -> None:  # noqa: N802
        self._dispatch("PUT")

    def do_DELETE(self) -> None:  # noqa: N802
        self._dispatch("DELETE")

    # -- routes -------------------------------------------------------------
    def _route(self, method: str) -> None:
        path = self._path()

        if method == "GET" and path in ("/healthz", "/"):
            self._send_json(200, {"ok": True, "service": "objstore"})
            return
        if method == "GET" and path == "/v1/usage":
            self._send_json(200, self.storage.usage())
            return
        if method == "GET" and path == "/v1/verify":
            report = self.storage.verify()
            self._send_json(200 if report["ok"] else 500, report)
            return
        if method == "POST" and path == "/v1/gc":
            body = self._json_body()
            self._send_json(200, self.storage.collect_garbage(force=bool(body.get("force"))))
            return

        if method == "POST" and path == "/v1/uploads":
            body = self._json_body()
            if "name" not in body or "length" not in body:
                raise ApiError(400, "BadRequest", "name and length are required")
            length = body["length"]
            if isinstance(length, bool) or not isinstance(length, int):
                raise ApiError(400, "BadRequest", "length must be an integer")
            self._send_json(201, self.storage.create_upload(body["name"], length))
            return

        m = _PATH_UPLOAD_COMPLETE.match(path)
        if m and method == "POST":
            sid = m.group(1)
            body = self._json_body()
            key = self.headers.get("Idempotency-Key") or body.get("idempotency_key")
            record, created = self.storage.complete_upload(sid, key)
            self._send_json(
                201 if created else 200,
                record,
                {"X-Version-Id": record["version_id"], "Location": f"/v1/versions/{record['version_id']}"},
            )
            return

        m = _PATH_UPLOAD_SEGMENT.match(path)
        if m and method == "PUT":
            sid, index = m.group(1), int(m.group(2))
            query = self._query()
            try:
                offset = int(query["offset"][0])
            except (KeyError, IndexError, ValueError):
                raise ApiError(400, "BadRequest", "offset query parameter is required") from None
            # A segment larger than the whole quota can never be accepted, so
            # reject it before buffering it; anything smaller is checked against
            # the *free* space inside the write transaction.
            declared = self.headers.get("Content-Length")
            if declared is not None and int(declared) > self.storage.quota:
                raise ApiError(
                    507, "QuotaExceeded", "segment exceeds the configured content quota"
                )
            data = self._read_body(self.storage.quota)
            digest = self.headers.get("X-Chunk-SHA256") or self.headers.get("X-Chunk-Digest")
            if digest is None:
                raise ApiError(400, "BadRequest", "X-Chunk-SHA256 header is required")
            digest = digest.strip().lower()
            if not re.fullmatch(r"[0-9a-f]{64}", digest):
                raise ApiError(400, "BadRequest", "X-Chunk-SHA256 must be 64 hex characters")
            if hashlib.sha256(data).hexdigest() != digest:
                raise ApiError(400, "BadDigest", "X-Chunk-SHA256 does not match the body")
            self._send_json(
                200, self.storage.put_segment(sid, index, offset, len(data), data, digest)
            )
            return

        m = _PATH_UPLOAD.match(path)
        if m and method == "GET":
            self._send_json(200, self.storage.upload_state(m.group(1)))
            return
        if m and method == "DELETE":
            self.storage.abort_upload(m.group(1))
            self.send_response(204)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        m = _PATH_VERSIONS.match(path)
        if m and method == "DELETE":
            self._send_json(200, self.storage.retire_object(m.group(1)))
            return
        if m and method == "GET":
            self._send_json(200, {"versions": self.storage.list_versions(m.group(1))})
            return

        m = _PATH_VERSION.match(path)
        if m and method == "GET":
            name, version_id = m.group(1), m.group(2)
            self._send_json(200, self.storage.get_version(version_id, name))
            return
        if m and method == "DELETE":
            name, version_id = m.group(1), m.group(2)
            self.storage.delete_version(name, version_id)
            self.send_response(204)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        m = _PATH_VERSION_BY_ID.match(path)
        if m and method == "GET":
            self._send_json(200, self.storage.get_version(m.group(1)))
            return

        m = _PATH_DOWNLOAD_VERSION.match(path)
        if m and method in ("GET", "HEAD"):
            self._stream_download(m.group(1), m.group(2), head=(method == "HEAD"))
            return
        m = _PATH_DOWNLOAD.match(path)
        if m and method in ("GET", "HEAD"):
            self._stream_download(m.group(1), None, head=(method == "HEAD"))
            return

        m = _PATH_VERSION_CONTENT.match(path)
        if m and method in ("GET", "HEAD"):
            self._stream_download(None, m.group(1), head=(method == "HEAD"))
            return

        if path.startswith("/v1/"):
            raise ApiError(404, "NotFound", f"no route for {method} {path}")
        raise ApiError(404, "NotFound", "unknown path")

    def _stream_download(self, name: Optional[str], version_id: Optional[str], head: bool) -> None:
        with self.storage.open_download(name, version_id) as (record, chunks):
            self._respond_started = True
            if head:
                self.send_response(200)
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Content-Length", str(record["size"]))
                self.send_header("X-Version-Id", record["version_id"])
                self.send_header("X-Object-Name", quote(record["name"]))
                self.end_headers()
                return
            try:
                self.send_response(200)
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Content-Length", str(record["size"]))
                self.send_header("X-Version-Id", record["version_id"])
                self.send_header("X-Object-Name", quote(record["name"]))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                for chunk in chunks:
                    self.wfile.write(chunk)
            except (BrokenPipeError, ConnectionResetError):
                self.close_connection = True


class Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address, storage: Storage, quiet: bool = True) -> None:
        self.storage = storage
        self.quiet = quiet
        super().__init__(address, Handler)


def serve(host: str, port: int, storage: Storage, quiet: bool = True) -> Server:
    return Server((host, port), storage, quiet=quiet)
