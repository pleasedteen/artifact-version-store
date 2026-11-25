"""Durable, quota-governed, versioned object storage.

Design in one paragraph
-----------------------
All mutable state lives in one SQLite database (WAL, ``synchronous=FULL``).
Object bytes live in separate files, written at absolute logical offsets, so a
long download never holds a database lock and cannot block publishers.  A
*stream* row is the unit of content (an upload being staged, or the content of a
published version).  Each stream has a manifest of ``(offset, length)`` extents
that is committed in the same transaction that confirms the bytes, and the bytes
are written + ``fsync``-ed before that commit.  Therefore:

* a hard kill may leave *unaccounted* bytes in a file, never accounted bytes that
  are missing; a retried segment simply overwrites them;
* quota (``SUM(stream.size)``) is charged in the transaction that makes bytes
  part of a stream, so concurrent writers can never overshoot it, and metadata
  (rows, indexes, the SQLite file) is never part of that number;
* publishing converts the staged stream in place.  It creates no second copy of
  the bytes, so it adds zero bytes, succeeds even when the quota is exactly
  full, and no publish-time temporary content can escape the quota;
* a content file is unlinked only *after* the transaction that deletes its
  stream row commits, and a stream is only deleted once it has no in-flight
  downloads.  A reader that already opened a retired version therefore always
  reaches the end of the file, and the bytes are reclaimed the moment the last
  such reader closes.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import secrets
import sqlite3
import threading
import time
from dataclasses import dataclass
from typing import Generator, Iterable, Optional

from .errors import ApiError

# Uploads abandoned by crashed clients are swept by the janitor after this long
# without a confirmed segment.  Configurable for tests; 10 minutes by default.
UPLOAD_IDLE_SWEEP = float(os.environ.get("OBJSTORE_UPLOAD_IDLE_SWEEP", 10 * 60.0))


class ContentUnavailable(Exception):
    """The bytes behind a stream can no longer be opened."""


def _fault(point: str) -> None:
    """Hard-kill the process at a named point (test fault fixture only).

    Enabled exclusively by ``OBJSTORE_FAULT_CRASH_AT``; never set in production.
    When ``OBJSTORE_FAULT_ARM_FILE`` is also set, the fault fires only once the
    test creates that file, and disarms itself before dying -- that lets a
    scenario run normal traffic first and then trigger the kill on the very
    next call.  ``os._exit`` skips ``finally`` blocks on purpose: it is a
    faithful stand-in for ``kill -9`` at the interesting moment of the commit
    protocol.
    """
    if os.environ.get("OBJSTORE_FAULT_CRASH_AT") != point:
        return
    arm = os.environ.get("OBJSTORE_FAULT_ARM_FILE")
    if arm:
        if not os.path.exists(arm):
            return
        with contextlib.suppress(FileNotFoundError):
            os.unlink(arm)
    os._exit(70)


def _json_extents(pairs: Iterable[tuple[int, int]]) -> str:
    # Sort by offset only: ``sorted`` on the pairs themselves would reorder the
    # two elements of a pair whenever ``length < offset``.
    ordered = sorted(((int(o), int(n)) for o, n in pairs), key=lambda p: p[0])
    return json.dumps([[o, n] for o, n in ordered], separators=(",", ":"))


def _parse_extents(raw: str) -> list[tuple[int, int]]:
    data = json.loads(raw) if raw else []
    return [(int(o), int(n)) for o, n in data]


def _segment_checksum(index: int, offset: int, length: int, digest: str) -> str:
    sha = hashlib.sha256()
    sha.update(f"{index}:{offset}:{length}:{digest}".encode())
    return sha.hexdigest()


def _now() -> float:
    return time.time()


def new_id(prefix: str = "") -> str:
    return prefix + secrets.token_hex(12)


def fold_extents(extents: Iterable[tuple[int, int]]) -> list[tuple[int, int]]:
    """Merge ``(offset, length)`` extents into sorted half-open ``(start, end)``
    spans.  Callers must not treat the result as ``(offset, length)`` pairs."""
    out: list[tuple[int, int]] = []
    for off, n in sorted((int(o), int(n)) for o, n in extents):
        start, end = off, off + n
        if out and start <= out[-1][1]:
            out[-1] = (out[-1][0], max(out[-1][1], end))
        else:
            out.append((start, end))
    return out


@dataclass(frozen=True)
class Intervals:
    """A sorted, merged set of half-open ``[start, end)`` ranges."""

    spans: tuple[tuple[int, int], ...]

    @classmethod
    def from_pairs(cls, pairs: Iterable[tuple[int, int]]) -> "Intervals":
        return cls(tuple(fold_extents(list(pairs))))

    def intersects(self, start: int, end: int) -> bool:
        """True if ``[start, end)`` overlaps any already-confirmed span."""
        if start == end:
            return False
        for s, e in self.spans:
            if e <= start:
                continue
            return s < end
        return False

    def contiguous_prefix(self, length: int) -> int:
        """Length of the confirmed prefix of ``[0, length)``."""
        pos = 0
        for s, e in self.spans:
            if s > pos:
                break
            if e > pos:
                pos = e
                if pos >= length:
                    return length
        return min(pos, length)


class Db:
    """SQLite wrapper; every state change runs under one process-wide lock."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.mu = threading.RLock()
        self.conn = sqlite3.connect(path, isolation_level=None, check_same_thread=False, timeout=60)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS obj (
                name TEXT PRIMARY KEY
            );
            CREATE TABLE IF NOT EXISTS stream (
                id         TEXT PRIMARY KEY,
                kind       TEXT NOT NULL,               -- upload | version
                name       TEXT NOT NULL,
                length     INTEGER NOT NULL DEFAULT 0,  -- declared total
                size       INTEGER NOT NULL DEFAULT 0,  -- bytes in the manifest
                extents    TEXT NOT NULL DEFAULT '[]',
                path       TEXT NOT NULL,
                refs       INTEGER NOT NULL DEFAULT 0,  -- in-flight downloads
                done       INTEGER NOT NULL DEFAULT 0,
                version_id TEXT,
                created    REAL NOT NULL,
                updated    REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS stream_name ON stream(name);
            CREATE TABLE IF NOT EXISTS segment (
                stream_id TEXT NOT NULL,
                idx       INTEGER NOT NULL,
                offset    INTEGER NOT NULL,
                length    INTEGER NOT NULL,
                digest    TEXT NOT NULL,
                checksum  TEXT NOT NULL,
                created   REAL NOT NULL,
                PRIMARY KEY (stream_id, idx)
            );
            CREATE TABLE IF NOT EXISTS version (
                id         TEXT PRIMARY KEY,
                name       TEXT NOT NULL,
                seq        INTEGER NOT NULL,
                stream_id  TEXT,
                size       INTEGER NOT NULL,
                active     INTEGER NOT NULL,
                reclaimed  INTEGER NOT NULL DEFAULT 0,
                created    REAL NOT NULL,
                retired    REAL
            );
            CREATE INDEX IF NOT EXISTS version_name ON version(name, active);
            CREATE TABLE IF NOT EXISTS completion (
                upload_id  TEXT PRIMARY KEY,
                version_id TEXT NOT NULL,
                created    REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS idem (
                key        TEXT PRIMARY KEY,
                upload_id  TEXT,
                version_id TEXT NOT NULL,
                created    REAL NOT NULL
            );
            """
        )

    @contextlib.contextmanager
    def write(self) -> Generator[sqlite3.Connection, None, None]:
        with self.mu:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                yield self.conn
            except BaseException:
                self.conn.execute("ROLLBACK")
                raise
            else:
                self.conn.execute("COMMIT")

    @contextlib.contextmanager
    def read(self) -> Generator[sqlite3.Connection, None, None]:
        with self.mu:
            yield self.conn

    # -- accounting ---------------------------------------------------------
    def content_bytes(self, c: sqlite3.Connection) -> int:
        row = c.execute("SELECT COALESCE(SUM(size), 0) AS total FROM stream").fetchone()
        return int(row["total"])

    @staticmethod
    def unlink(paths: Iterable[str]) -> None:
        for p in paths:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(p)

    def gc(self, c: sqlite3.Connection) -> list[str]:
        """Reclaim retired streams with no readers.  Returns files to unlink.

        The caller unlinks *after* the transaction commits, so a crash can
        never leave a row pointing at bytes that were already removed.
        """
        paths: list[str] = []
        rows = c.execute(
            "SELECT v.id AS version_id, v.stream_id AS stream_id, s.refs AS refs, s.path AS path"
            " FROM version v JOIN stream s ON s.id = v.stream_id"
            " WHERE v.active = 0 AND v.retired IS NOT NULL"
        ).fetchall()
        for row in rows:
            if int(row["refs"]) > 0:
                continue
            paths.append(row["path"])
            c.execute("DELETE FROM segment WHERE stream_id = ?", (row["stream_id"],))
            c.execute("DELETE FROM stream WHERE id = ?", (row["stream_id"],))
            c.execute(
                "UPDATE version SET stream_id = NULL, reclaimed = 1 WHERE id = ?",
                (row["version_id"],),
            )
        return paths


class Storage:
    def __init__(self, data_dir: str, quota: int) -> None:
        self.data_dir = os.path.abspath(data_dir)
        self.content_dir = os.path.join(self.data_dir, "content")
        self.db_dir = os.path.join(self.data_dir, "db")
        os.makedirs(self.content_dir, exist_ok=True)
        os.makedirs(self.db_dir, exist_ok=True)
        self.quota = int(quota)
        if self.quota < 0:
            raise ValueError("quota must be >= 0")
        self.db = Db(os.path.join(self.db_dir, "state.sqlite3"))
        self._recover()
        self._janitor_stop = threading.Event()
        self._janitor = threading.Thread(target=self._janitor_loop, daemon=True, name="janitor")
        self._janitor.start()

    # ------------------------------------------------------------------ setup
    def _recover(self) -> None:
        """Startup recovery after a hard kill.

        * No download can be in flight in a process that just started, so every
          persisted pin is stale; clearing them lets retired content be freed.
        * Retired streams with no readers are reclaimed.
        * Bytes past a stream's manifest (written but never committed) are
          truncated away, so the quota cannot be silently overrun by them.
        """
        with self.db.write() as c:
            c.execute("UPDATE stream SET refs = 0")
            freed = self.db.gc(c)
            self._reclaim_crash_tails(c)
            known = {r["path"] for r in c.execute("SELECT path FROM stream").fetchall()}
        self.db.unlink(freed)
        for entry in os.listdir(self.content_dir):
            path = os.path.join(self.content_dir, entry)
            if path not in known:
                self.db.unlink([path])

    def _new_stream(self, c: sqlite3.Connection, kind: str, name: str, length: int = 0) -> str:
        sid = new_id("s_")
        path = os.path.join(self.content_dir, sid + ".part")
        now = _now()
        c.execute(
            "INSERT INTO stream (id, kind, name, length, size, extents, path, refs, done,"
            " version_id, created, updated)"
            " VALUES (?,?,?,?,0,'[]',?,0,0,NULL,?,?)",
            (sid, kind, name, length, path, now, now),
        )
        return sid

    def _stream(self, c: sqlite3.Connection, sid: str) -> sqlite3.Row:
        row = c.execute("SELECT * FROM stream WHERE id = ?", (sid,)).fetchone()
        if row is None:
            raise ApiError(404, "NotFound", f"upload {sid} does not exist")
        return row

    def _free(self, c: sqlite3.Connection) -> int:
        return self.quota - self.db.content_bytes(c)

    # ---------------------------------------------------------------- uploads
    def create_upload(self, name: str, length: int) -> dict:
        if not isinstance(name, str) or not name:
            raise ApiError(400, "BadRequest", "name must be a non-empty string")
        if not isinstance(length, int) or isinstance(length, bool) or length < 0:
            raise ApiError(400, "BadRequest", "length must be a non-negative integer")
        with self.db.write() as c:
            c.execute("INSERT OR IGNORE INTO obj (name) VALUES (?)", (name,))
            sid = self._new_stream(c, "upload", name, length)
            return self._upload_state(c, sid)

    def abort_upload(self, sid: str) -> bool:
        """Abandon an unfinished upload and free its staged bytes immediately."""
        freed: list[str] = []
        with self.db.write() as c:
            s = self._stream(c, sid)
            if s["kind"] != "upload":
                raise ApiError(409, "NotAnUpload", "content is published; nothing to abort")
            c.execute("DELETE FROM segment WHERE stream_id = ?", (sid,))
            c.execute("DELETE FROM stream WHERE id = ?", (sid,))
            freed = [s["path"]]
        self.db.unlink(freed)
        return True

    def upload_state(self, sid: str) -> dict:
        with self.db.read() as c:
            return self._upload_state(c, sid)

    def _upload_state(self, c: sqlite3.Connection, sid: str) -> dict:
        s = self._stream(c, sid)
        segs = c.execute(
            "SELECT idx, offset, length, digest FROM segment WHERE stream_id = ? ORDER BY idx",
            (sid,),
        ).fetchall()
        total = int(s["length"])
        intervals = Intervals.from_pairs(_parse_extents(s["extents"]))
        return {
            "upload_id": sid,
            "name": s["name"],
            "length": total,
            "confirmed": intervals.contiguous_prefix(total),
            "confirmed_ranges": [{"offset": a, "length": b - a} for a, b in intervals.spans],
            "durable_bytes": int(s["size"]),
            "segments": [
                {"index": r["idx"], "offset": r["offset"], "length": r["length"], "digest": r["digest"]}
                for r in segs
            ],
            "complete": bool(s["done"]),
        }

    def put_segment(
        self, sid: str, index: int, offset: int, length: int, data: bytes, digest: str
    ) -> dict:
        if len(data) != length:
            raise ApiError(400, "BadRequest", "body length does not match declared length")
        with self.db.write() as c:
            s = self._stream(c, sid)
            if s["kind"] != "upload":
                raise ApiError(409, "NotAnUpload", "content is published; upload is closed")
            if s["done"]:
                raise ApiError(409, "UploadComplete", "upload is already finished")
            total = int(s["length"])
            if offset < 0 or length <= 0 or offset + length > total:
                raise ApiError(400, "OutOfRange", "segment is outside the declared length")
            existing = c.execute(
                "SELECT offset, length, checksum FROM segment WHERE stream_id = ? AND idx = ?",
                (sid, index),
            ).fetchone()
            want = _segment_checksum(index, offset, length, digest)
            if existing is not None:
                if (
                    int(existing["offset"]) == offset
                    and int(existing["length"]) == length
                    and existing["checksum"] == want
                ):
                    return self._upload_state(c, sid)  # idempotent retry
                raise ApiError(
                    409, "SegmentConflict", f"segment {index} is already confirmed with other content"
                )
            extents = _parse_extents(s["extents"])
            if Intervals.from_pairs(extents).intersects(offset, offset + length):
                raise ApiError(409, "SegmentConflict", "range overlaps an already-confirmed segment")
            if length > self._free(c):
                raise ApiError(507, "QuotaExceeded", "content quota exceeded")
            # Bytes are made durable *before* the manifest that accounts for them.
            created = not os.path.exists(s["path"])
            fd = os.open(s["path"], os.O_WRONLY | os.O_CREAT, 0o644)
            try:
                view = memoryview(data)
                pos = offset
                while view:
                    n = os.pwrite(fd, view, pos)
                    view = view[n:]
                    pos += n
                os.fsync(fd)
            finally:
                os.close(fd)
            if created:
                self._fsync_dir()
            _fault("segment_after_fsync")
            extents.append((offset, length))
            # Persist the merged, non-overlapping spans, never the raw list of
            # appends: merging keeps the manifest canonical and prevents
            # adjacent pieces from being described twice.
            merged = fold_extents(extents)
            c.execute(
                "UPDATE stream SET size = size + ?, extents = ?, updated = ? WHERE id = ?",
                (length, _json_extents([(a, b - a) for a, b in merged]), _now(), sid),
            )
            c.execute(
                "INSERT INTO segment (stream_id, idx, offset, length, digest, checksum, created)"
                " VALUES (?,?,?,?,?,?,?)",
                (sid, index, offset, length, digest, want, _now()),
            )
            return self._upload_state(c, sid)

    def _fsync_dir(self) -> None:
        fd = os.open(self.content_dir, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def complete_upload(self, sid: str, idem_key: Optional[str]) -> tuple[dict, bool]:
        """Publish a fully received upload.  Returns ``(record, created)``.

        A retry -- because the first response was lost -- returns the very same
        version record, whether it is keyed by ``Idempotency-Key`` or by the
        upload id itself.
        """
        with self.db.write() as c:
            # Retries are answered from the completion ledger even if the
            # version has since been taken down and reclaimed, so a lost
            # response can always be resolved to the original version id.
            row = c.execute(
                "SELECT version_id FROM completion WHERE upload_id = ?", (sid,)
            ).fetchone()
            if row is not None:
                return self._version_record(c, row["version_id"]), False
            if idem_key is not None:
                row = c.execute(
                    "SELECT upload_id, version_id FROM idem WHERE key = ?", (idem_key,)
                ).fetchone()
                if row is not None:
                    if row["upload_id"] not in (None, sid):
                        raise ApiError(
                            409, "IdempotencyConflict",
                            "this Idempotency-Key belongs to another upload",
                        )
                    return self._version_record(c, row["version_id"]), False
            s = self._stream(c, sid)  # unknown upload -> 404
            if s["kind"] != "upload":
                raise ApiError(409, "NotAnUpload", "stream is not an upload")
            total = int(s["length"])
            intervals = Intervals.from_pairs(_parse_extents(s["extents"]))
            if total > 0 and intervals.contiguous_prefix(total) < total:
                raise ApiError(409, "Incomplete", "upload is not complete")
            if int(s["size"]) != total:
                raise ApiError(409, "Incomplete", "upload byte count does not match its length")
            version_id = new_id("v_")
            seq = c.execute(
                "SELECT COALESCE(MAX(seq), 0) + 1 FROM version WHERE name = ?", (s["name"],)
            ).fetchone()[0]
            # Publish in place: the staged bytes are already charged to the
            # quota, so this adds none and succeeds at a completely full quota.
            c.execute(
                "UPDATE stream SET kind = 'version', version_id = ?, done = 1, updated = ?"
                " WHERE id = ?",
                (version_id, _now(), sid),
            )
            c.execute(
                "INSERT INTO version (id, name, seq, stream_id, size, active, reclaimed, created, retired)"
                " VALUES (?,?,?,?,?,1,0,?,NULL)",
                (version_id, s["name"], seq, sid, total, _now()),
            )
            c.execute(
                "INSERT INTO completion (upload_id, version_id, created) VALUES (?,?,?)",
                (sid, version_id, _now()),
            )
            if idem_key is not None:
                c.execute(
                    "INSERT OR IGNORE INTO idem (key, upload_id, version_id, created)"
                    " VALUES (?,?,?,?)",
                    (idem_key, sid, version_id, _now()),
                )
            # Superseding the name pointer is one atomic step with the publish,
            # so "last effective version" is never ambiguous.  The previous
            # version is NOT retired: it stays downloadable by version id until
            # an explicit take-down (DELETE) retires it.
            c.execute(
                "UPDATE version SET active = 0 WHERE name = ? AND active = 1 AND id != ?",
                (s["name"], version_id),
            )
            freed = self.db.gc(c)
            record = self._version_record(c, version_id)
        # Fault point: the commit is durable, the response has not been sent.
        _fault("complete_after_commit")
        self.db.unlink(freed)
        return record, True

    def _version_record(self, c: sqlite3.Connection, version_id: str) -> dict:
        row = c.execute(
            "SELECT id, name, seq, active, reclaimed, created, retired, size, stream_id"
            " FROM version WHERE id = ?",
            (version_id,),
        ).fetchone()
        if row is None:
            raise ApiError(404, "NotFound", f"version {version_id} does not exist")
        return {
            "version_id": row["id"],
            "name": row["name"],
            "seq": row["seq"],
            "size": int(row["size"]),
            "active": bool(row["active"]),
            "reclaimed": bool(row["reclaimed"]),
            "created_at": row["created"],
            "retired_at": row["retired"],
        }

    def get_version(self, version_id: str, name: Optional[str] = None) -> dict:
        with self.db.read() as c:
            record = self._version_record(c, version_id)
            if name is not None and record["name"] != name:
                raise ApiError(404, "NotFound", f"version {version_id} does not belong to {name!r}")
            return record

    def list_versions(self, name: str) -> list[dict]:
        with self.db.read() as c:
            ids = [
                r["id"]
                for r in c.execute(
                    "SELECT id FROM version WHERE name = ? ORDER BY seq DESC", (name,)
                ).fetchall()
            ]
            return [self._version_record(c, i) for i in ids]

    def _resolve(self, c: sqlite3.Connection, name: Optional[str], version_id: Optional[str]) -> sqlite3.Row:
        base = (
            "SELECT v.id, v.name, v.seq, v.active, v.reclaimed, v.retired, v.size AS content_size,"
            " v.stream_id AS stream_id, s.path AS content_path"
            " FROM version v LEFT JOIN stream s ON s.id = v.stream_id"
        )
        if version_id is not None:
            row = c.execute(base + " WHERE v.id = ?", (version_id,)).fetchone()
            if row is None or (name is not None and row["name"] != name):
                raise ApiError(404, "NotFound", f"version {version_id} does not exist")
        else:
            row = c.execute(base + " WHERE v.name = ? AND v.active = 1", (name,)).fetchone()
            if row is None:
                raise ApiError(404, "NotFound", f"no published version for {name!r}")
        if row["retired"] is not None or row["reclaimed"] or row["content_path"] is None:
            raise ApiError(410, "Gone", f"version {row['id']} has been retired")
        return row

    def delete_version(self, name: str, version_id: str) -> bool:
        """Retire a version (idempotent).  In-flight readers keep their bytes."""
        freed: list[str] = []
        with self.db.write() as c:
            row = c.execute(
                "SELECT id, stream_id, active, retired FROM version WHERE name = ? AND id = ?",
                (name, version_id),
            ).fetchone()
            if row is None:
                raise ApiError(404, "NotFound", f"no version {version_id} for {name!r}")
            if row["retired"] is not None:
                return True  # already taken down; retiring is idempotent
            # No promotion of an older version: "the last effective publish" is
            # a monotonic notion, so taking down the newest one leaves the name
            # unpublished (404) instead of silently resurrecting stale bytes.
            c.execute(
                "UPDATE version SET active = 0, retired = ? WHERE id = ?", (_now(), version_id)
            )
            freed = self.db.gc(c)
        self.db.unlink(freed)
        return True

    def retire_object(self, name: str) -> dict:
        """Take down every version of ``name`` (the cleanup-task primitive)."""
        with self.db.write() as c:
            rows = c.execute(
                "SELECT id FROM version WHERE name = ? AND retired IS NULL", (name,)
            ).fetchall()
            if not rows and c.execute("SELECT 1 FROM obj WHERE name = ?", (name,)).fetchone() is None:
                raise ApiError(404, "NotFound", f"no object named {name!r}")
            now = _now()
            for row in rows:
                c.execute("UPDATE version SET active = 0, retired = ? WHERE id = ?", (now, row["id"]))
            freed = self.db.gc(c)
            remaining = [
                self._version_record(c, r["id"])
                for r in c.execute("SELECT id FROM version WHERE name = ? ORDER BY seq", (name,)).fetchall()
            ]
        self.db.unlink(freed)
        return {"name": name, "retired": len(rows), "versions": remaining}

    # ------------------------------------------------------------- downloads
    @contextlib.contextmanager
    def open_download(self, name: Optional[str], version_id: Optional[str]):
        """Pin one version, yield ``(record, chunk_iterator)``; unpin on exit.

        The version is resolved exactly once, under the write lock, and its
        stream is pinned.  Whatever a concurrent publish or retire does, this
        reader keeps its file alive until the iterator is closed.
        """
        if name is None and version_id is None:
            raise ApiError(400, "BadRequest", "either name or version_id is required")
        with self.db.write() as c:
            row = self._resolve(c, name, version_id)
            stream_id = row["stream_id"]
            extents = _parse_extents(
                c.execute("SELECT extents FROM stream WHERE id = ?", (stream_id,)).fetchone()["extents"]
            )
            path = row["content_path"]
            c.execute("UPDATE stream SET refs = refs + 1 WHERE id = ?", (stream_id,))
            chosen = {
                "version_id": row["id"],
                "name": row["name"],
                "seq": row["seq"],
                "size": int(row["content_size"]),
                "active": bool(row["active"]),
            }
        fd = None
        try:
            fd = os.open(path, os.O_RDONLY)
        except FileNotFoundError:
            if chosen["size"] != 0:
                with self.db.write() as c:
                    c.execute("UPDATE stream SET refs = refs - 1 WHERE id = ?", (stream_id,))
                    freed = self.db.gc(c)
                self.db.unlink(freed)
                raise ContentUnavailable(path) from None
        try:
            yield chosen, self._read_extents(fd, extents if fd is not None else [])
        finally:
            if fd is not None:
                os.close(fd)
            with self.db.write() as c:
                c.execute("UPDATE stream SET refs = refs - 1 WHERE id = ?", (stream_id,))
                freed = self.db.gc(c)
            self.db.unlink(freed)

    @staticmethod
    def _read_extents(fd: int, extents: list[tuple[int, int]], chunk: int = 65536):
        if fd is None:
            return
        for off, length in extents:
            os.lseek(fd, off, os.SEEK_SET)
            remaining = length
            while remaining:
                data = os.read(fd, min(chunk, remaining))
                if not data:
                    raise ContentUnavailable(f"short read at offset {off}")
                remaining -= len(data)
                yield data

    # ------------------------------------------------------------ housekeeping
    def usage(self) -> dict:
        with self.db.read() as c:
            used = self.db.content_bytes(c)
            by_kind = {
                r["kind"]: int(r["total"] or 0)
                for r in c.execute(
                    "SELECT kind, SUM(size) AS total FROM stream GROUP BY kind"
                ).fetchall()
            }
            active = c.execute(
                "SELECT COALESCE(SUM(s.size), 0) FROM version v JOIN stream s ON s.id = v.stream_id"
                " WHERE v.active = 1"
            ).fetchone()[0]
            retained = c.execute(
                "SELECT COALESCE(SUM(s.size), 0) FROM version v JOIN stream s ON s.id = v.stream_id"
                " WHERE v.active = 0 AND v.retired IS NULL"
            ).fetchone()[0]
            retired = c.execute(
                "SELECT COALESCE(SUM(s.size), 0) FROM version v JOIN stream s ON s.id = v.stream_id"
                " WHERE v.retired IS NOT NULL"
            ).fetchone()[0]
            open_downloads = c.execute("SELECT COALESCE(SUM(refs), 0) FROM stream").fetchone()[0]
            return {
                "quota_bytes": self.quota,
                "used_bytes": used,
                "free_bytes": self.quota - used,
                "staged_bytes": by_kind.get("upload", 0),
                "active_bytes": int(active),
                "retained_bytes": int(retained),
                "retiring_bytes": int(retired),
                "open_downloads": int(open_downloads),
                "by_kind": by_kind,
                "metadata": self._metadata(c),
            }

    def _metadata(self, c: sqlite3.Connection) -> dict:
        return {
            "objects": c.execute("SELECT COUNT(*) FROM obj").fetchone()[0],
            "versions": c.execute("SELECT COUNT(*) FROM version").fetchone()[0],
            "reclaimed_versions": c.execute("SELECT COUNT(*) FROM version WHERE reclaimed = 1").fetchone()[0],
            "uploads": c.execute("SELECT COUNT(*) FROM stream WHERE kind = 'upload'").fetchone()[0],
            "segments": c.execute("SELECT COUNT(*) FROM segment").fetchone()[0],
            "streams": c.execute("SELECT COUNT(*) FROM stream").fetchone()[0],
            "content_files": len(os.listdir(self.content_dir)),
            "db_bytes": sum(
                os.path.getsize(os.path.join(self.db_dir, f))
                for f in os.listdir(self.db_dir)
                if os.path.isfile(os.path.join(self.db_dir, f))
            ),
        }

    def collect_garbage(self, force: bool = False) -> dict:
        """Drop idle uploads and any file no stream row owns.

        ``force`` also drops uploads that are fresh but have not finished.
        """
        now = _now()
        freed = 0
        removed = 0
        freed_paths: list[str] = []
        with self.db.write() as c:
            stale = c.execute(
                "SELECT id, path, size FROM stream WHERE kind = 'upload' AND done = 0"
                + ("" if force else " AND updated < ?"),
                () if force else (now - UPLOAD_IDLE_SWEEP,),
            ).fetchall()
            for row in stale:
                c.execute("DELETE FROM segment WHERE stream_id = ?", (row["id"],))
                c.execute("DELETE FROM stream WHERE id = ?", (row["id"],))
                freed += int(row["size"])
                removed += 1
                freed_paths.append(row["path"])
            freed_paths.extend(self.db.gc(c))
            # A file whose row never committed (kill inside the create path) has
            # no owner.  The scan runs under the write lock so it cannot race an
            # uploader that is creating its file right now.
            known = {r["path"] for r in c.execute("SELECT path FROM stream").fetchall()}
            for entry in os.listdir(self.content_dir):
                p = os.path.join(self.content_dir, entry)
                if p not in known and not p.endswith(".compact"):
                    with contextlib.suppress(FileNotFoundError):
                        os.unlink(p)
                        removed += 1
        self.db.unlink(freed_paths)
        return {"reclaimed_uploads": removed, "reclaimed_bytes": freed}

    def _reclaim_crash_tails(self, c: sqlite3.Connection) -> None:
        """Shrink each stream file back to the end of its manifest.

        The only way bytes reach a file without a committed manifest entry is a
        kill between ``pwrite`` and the commit.  Those bytes lie at or beyond the
        furthest confirmed extent, so truncating there removes them.  Gaps
        *before* that point are sparse holes (they occupy no disk blocks) and
        keep their logical offsets, so a retried segment still lands exactly
        where the client says it should.  Startup only: no writers exist yet.
        """
        for row in c.execute("SELECT id, path, extents FROM stream").fetchall():
            end = max((o + n for o, n in _parse_extents(row["extents"])), default=0)
            try:
                fd = os.open(row["path"], os.O_RDWR)
            except FileNotFoundError:
                if end:
                    raise RuntimeError(f"stream {row['id']}: content file is missing") from None
                continue
            try:
                if os.fstat(fd).st_size > end:
                    os.ftruncate(fd, end)
                    os.fsync(fd)
            finally:
                os.close(fd)

    def verify(self) -> dict:
        """Offline consistency check: manifest vs. bytes on disk vs. quota.

        Walks every live stream, sizes its file, and recomputes the quota.  Used
        by the test suite after crash scenarios; safe to call while serving.
        """
        problems: list[str] = []
        total = 0
        with self.db.read() as c:
            rows = c.execute("SELECT id, kind, size, extents, path FROM stream").fetchall()
            for row in rows:
                extents = _parse_extents(row["extents"])
                accounted = sum(n for _, n in extents)
                if accounted != int(row["size"]):
                    problems.append(f"stream {row['id']}: extents={accounted} size={row['size']}")
                total += accounted
                try:
                    on_disk = os.path.getsize(row["path"])
                except FileNotFoundError:
                    if accounted:
                        problems.append(f"stream {row['id']}: file missing")
                    continue
                end = max((o + n for o, n in _parse_extents(row["extents"])), default=0)
                if on_disk > end:
                    problems.append(f"stream {row['id']}: {on_disk - end} unaccounted bytes")
            quota_rows = c.execute("SELECT COALESCE(SUM(size), 0) FROM stream").fetchone()[0]
            if int(quota_rows) != total:
                problems.append("quota sum disagrees with stream manifests")
        return {"ok": not problems, "content_bytes": total, "problems": problems}

    def _janitor_loop(self) -> None:
        while not self._janitor_stop.wait(2.0):
            with contextlib.suppress(Exception):
                self.collect_garbage()

    def close(self) -> None:
        self._janitor_stop.set()
        self._janitor.join(timeout=5)
        with self.db.mu:
            self.db.conn.close()
