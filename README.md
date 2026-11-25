# objstore — single-node, versioned, quota-governed artifact store

A local HTTP object store for CI build nodes, written in pure Python (standard
library only, SQLite for state). Build agents upload artifacts in segments under
the same name, one batch after another; on-call engineers download a whole batch
by name or by exact version; a cleanup job retires old versions and frees disk
space **while the service is running**.

```bash
# start (defaults: 127.0.0.1:8080, ./objstore-data, 64 MiB of content)
python3 -m objstore.serve

# explicit
python3 -m objstore.serve --host 127.0.0.1 --port 8080 \
    --data-dir /var/lib/objstore --quota 2G

# run the self-checks (one command, ~16 s, well under the 80 s budget)
python3 tests/run_tests.py
```

The server prints one JSON line when it is ready:

```json
{"event":"listening","host":"127.0.0.1","port":8080,"data_dir":"/...","quota_bytes":67108864}
```

`--quota` accepts bytes (`1048576`) or suffixes (`512K`, `64M`, `2G`, 1024-based).
`--print-config` resolves arguments/environment into JSON and exits.

---

## Model

| Concept | Meaning |
|---|---|
| **object name** | Free-form key, may contain `/` and non-ASCII (percent-encoded in URLs). |
| **upload** | A declared-length transfer, received as independent segments. |
| **version** | An immutable snapshot created by one successful completion of an upload. |
| **current version** | The most recently published, not-retired version of a name. |

Rules that hold at all times:

* An upload is invisible to downloads until it is **published**.
* Publishing supersedes the previous *active* pointer but does **not** delete the
  older versions: they stay fetchable by id.
* Every publish creates a **new version id**, even for identical bytes.
* A version becomes **retired** only through an explicit `DELETE` (cleanup). New
  downloads of a retired version are refused with `410`; downloads that had
  already started read to the end, and only then are the bytes reclaimed.
* All content bytes — staged uploads, the current version, superseded versions,
  retired-but-still-being-read content — count against one byte quota.
  Metadata (SQLite rows, indexes, the database file) is reported separately and
  never counted against it.

---

## HTTP API

All request/response bodies are JSON unless noted. Errors have the shape

```json
{"error": {"code": "QuotaExceeded", "message": "...", "retryable": false}}
```

### Create an upload

```http
POST /v1/uploads
Content-Type: application/json

{"name": "nightly/build.tar", "length": 10485760}
```

`201 Created`

```json
{
  "upload_id": "s_1f0c…",
  "name": "nightly/build.tar",
  "length": 10485760,
  "confirmed": 0,
  "confirmed_ranges": [],
  "durable_bytes": 0,
  "segments": [],
  "complete": false
}
```

### Upload a segment

```http
PUT /v1/uploads/{upload_id}/segments/{index}?offset=1048576
Content-Length: 1048576
X-Chunk-SHA256: <lowercase hex SHA-256 of the body>

<raw bytes>
```

The body must be exactly `Content-Length` bytes of the object starting at
`offset`; the digest header must match the body (`400 BadDigest` otherwise).

* Repeating **the same** `index`, `offset`, length and digest: `200`, idempotent
  no-op — retries are always safe.
* Reusing an `index` (or an overlapping byte range) with **different** content:
  `409 SegmentConflict`.
* `offset + length` outside the declared total: `400 OutOfRange`.
* Not enough free content quota: `507 QuotaExceeded` (nothing is stored).

The response is the full upload state (same shape as `GET /v1/uploads/{id}`),
including `confirmed`, the length of the contiguous prefix already durable —
this is the resume position. `confirmed_ranges` lists every durable range, so a
client may also fill gaps out of order. `durable_bytes` is the sum of all
confirmed ranges.

### Inspect an upload / resume point

```http
GET /v1/uploads/{upload_id}
```

### Abort an upload

```http
DELETE /v1/uploads/{upload_id}   →  204
```

Releases its staged bytes immediately. Published uploads cannot be aborted
(`409`).

### Publish (complete)

```http
POST /v1/uploads/{upload_id}/complete
Idempotency-Key: optional-client-chosen-key
```

`201 Created` with `X-Version-Id` and `Location: /v1/versions/{id}`:

```json
{
  "version_id": "v_9a3d…",
  "name": "nightly/build.tar",
  "seq": 7,
  "size": 10485760,
  "active": true,
  "reclaimed": false,
  "created_at": 1763…,
  "retired_at": null
}
```

* Not all bytes received: `409 Incomplete`.
* **Retry after a lost response** — whether the process was killed between the
  commit and the response, or the connection simply dropped — returns `200` and
  the *same* `version_id`; nothing new is published. This holds for the upload id
  itself and, additionally, for a supplied `Idempotency-Key` (reusing a key for a
  different upload is `409 IdempotencyConflict`).
* Publishing **never needs extra quota**: it charges nothing new, so a fully
  received upload publishes even when `used_bytes == quota_bytes`.

### Download

```http
GET  /v1/objects/{name}/content                      # current version
GET  /v1/objects/{name}/content/{version_id}         # specific version
GET  /v1/versions/{version_id}/content               # specific version, by id alone
HEAD …                                               # headers only
```

Responses carry `X-Version-Id`, `X-Object-Name`, `Content-Length`.
`404` when the name has no current version; `410 Gone` when that exact version
was retired (whether or not its bytes are still pinned by another reader).

The version is bound at the **start** of the response. A reader that pauses
mid-body keeps its version — concurrent publishes of the same name and
take-downs of the old one neither block it nor change its bytes.

### Version metadata and take-down

```http
GET    /v1/objects/{name}/versions                  → {"versions": […newest first…]}
GET    /v1/objects/{name}/versions/{version_id}     → version record
GET    /v1/versions/{version_id}                    → version record
DELETE /v1/objects/{name}/versions/{version_id}     → 204, retire this version
DELETE /v1/objects/{name}/versions                  → 200, retire every live version
```

The bulk form is the cleanup job's primitive:

```json
{"name": "nightly/build.tar", "retired": 12, "versions": [ … ]}
```

Retiring is idempotent. Bytes are unlinked the moment the last reader of that
version closes, not before.

### Operations

```http
GET  /healthz                     → {"ok": true}
GET  /v1/usage                    → quota, used/free bytes split by staged /
                                    active / retained / retiring, metadata counters
POST /v1/gc   {"force": false}    → reclaim abandoned uploads and orphan files
GET  /v1/verify                   → self-consistency report (manifest vs. disk vs. quota)
```

`/v1/usage` is what a controller should watch: `used_bytes` is the number the
quota enforces, `metadata.db_bytes` and `metadata.segments` are reported apart,
`open_downloads` shows pinned readers, and `retiring_bytes` shows content that is
retired but still being read.

---

## Worked example

```bash
B=http://127.0.0.1:8080
NAME=nightly/build.tar
D=$(mktemp -d)
LEN=$(stat -c%s build.tar)
UP=$(curl -s -XPOST $B/v1/uploads -H 'Content-Type: application/json' \
       -d "{\"name\":\"$NAME\",\"length\":$LEN}" \
     | python3 -c 'import sys,json;print(json.load(sys.stdin)["upload_id"])')

# Upload in 4 MiB pieces. Re-sending an identical piece is always safe.
split -b 4M build.tar $D/piece.
i=0; off=0
for f in $D/piece.*; do
  curl -s -XPUT "$B/v1/uploads/$UP/segments/$i?offset=$off" \
    -H "X-Chunk-SHA256: $(sha256sum "$f" | cut -d' ' -f1)" \
    --data-binary @"$f" >/dev/null
  off=$((off + $(stat -c%s "$f"))); i=$((i + 1))
done

# Where would a resumed client continue?
curl -s $B/v1/uploads/$UP | python3 -c 'import sys,json;print(json.load(sys.stdin)["confirmed"])'

# Publish (safe to repeat; a retry returns this same id), then download.
VER=$(curl -s -XPOST $B/v1/uploads/$UP/complete \
        | python3 -c 'import sys,json;print(json.load(sys.stdin)["version_id"])')
curl -s -o downloaded.tar $B/v1/objects/$NAME/content
curl -s -o pinned.tar     $B/v1/objects/$NAME/content/$VER

# Cleanup: take down every generation. Readers already downloading still finish.
curl -s -XDELETE $B/v1/objects/$NAME/versions
curl -s -o /dev/null -w '%{http_code}\n' $B/v1/objects/$NAME/content/$VER   # 410
```

---

## Guarantees and how they are met

* **Durability, not just persistence.** A segment is acknowledged only after its
  bytes are `fsync`-ed *and* the manifest that accounts for them is committed
  (SQLite WAL, `synchronous=FULL`). A `SIGKILL` can leave *unaccounted* bytes in
  a file; it can never leave accounted bytes missing. Startup truncates each file
  back to its manifest and the client re-sends the uncommitted segment.
* **Retry semantics.** Segment identity is `(index, offset, length, sha256)`, so
  an identical retry is a no-op and a changed retry is rejected — including a
  different index covering the same byte range.
* **Exactly-once publication.** Version ids are minted and recorded in a
  `completion` ledger inside the publish transaction; a retried completion reads
  the ledger first. This survives a kill after commit and a full restart.
* **Quota is a hard invariant, not an estimate.** The check and the charge happen
  in the same write transaction that adds bytes to a manifest, under one
  process-wide write lock. Concurrent uploaders race for the last bytes and
  exactly one wins; `used_bytes` is computed as `SUM(stream.size)`.
* **Publish at a full quota.** Bytes are staged once and published in place, so
  a completion copies nothing. (If a temporary publish copy existed, it would be
  a stream row and therefore charged to the same quota.)
* **Download pinning.** A download increments the stream's `refs` in the same
  transaction that resolves the version, and decrements it when the response
  ends. Reclamation deletes a stream row only when `refs = 0`; the file is
  unlinked only after that transaction commits. Open readers therefore hold a
  live inode and always finish; the disk space returns as they close.
* **Publishing never waits on readers.** Download bodies are read from a file
  descriptor, not under the database lock, so a slow client cannot block a
  publisher.
* **Crash recovery is automatic** at startup: stale pins are cleared, retired
  streams are reclaimed, files with no owning row are removed, and files are
  truncated to their manifests. `/v1/verify` re-checks all three afterwards.

## Layout

```
objstore/
  storage.py     engine: SQLite state, content files, quota, pinning, recovery
  http_api.py    HTTP surface and streaming downloads
  serve.py       CLI entry point (python -m objstore.serve)
  errors.py      ApiError → HTTP status + JSON body
tests/
  run_tests.py   the one-command self-check (35 checks, ~16 s)
  harness.py     subprocess server, HTTP client, pausable downloader
  fixtures.py    bundled batch data and the two fault points
```

Data directory layout: `db/state.sqlite3` (+ WAL/SHM) and `content/*.part`,
one file per live content stream, written at logical offsets.

## Self-check

```bash
python3 tests/run_tests.py             # coloured, human-readable
python3 tests/run_tests.py --quiet     # one line per scenario
python3 tests/run_tests.py --filter crash
python3 tests/run_tests.py --json report.json
```

Every scenario runs the real service as a **subprocess over real HTTP** with a
fresh data directory, so fault scenarios can `SIGKILL` it. Covered:

* declare → out-of-order segments → resume point → publish → download (by name,
  by version, by version id, HEAD);
* identical segment retry accepted, changed-content retry rejected, unpublished
  content invisible, digest mismatch rejected;
* three fixture batches published under one name: distinct ids, name resolves the
  newest, older ids still fetchable, listing newest-first;
* **lost completion response**: the process dies after commit, restarts, and the
  retried completion returns the original version id (also with
  `Idempotency-Key`);
* **forced-kill recovery**: confirmed segments survive `SIGKILL`, upload resumes,
  publish completes, bytes verify; a second kill keeps the published version;
* **kill between fsync and manifest commit**: the uncommitted segment is not
  counted, not visible, and retryable;
* retirement: refused for new downloads (`410`), a paused download still reads to
  the end, bytes reclaimed only when it closes;
* quota: staged bytes count, over-quota refused, publish at a completely full
  quota succeeds, metadata excluded, ten concurrent racers never overshoot,
  abort frees space;
* **download vs. publish**: a 1 MiB download starts on v1, v2 publishes in ~50 ms
  meanwhile, the reader still finishes on v1;
* **download vs. reclaim**: four paused readers, all four versions retired, all
  four finish byte-perfect, then the quota returns to zero;
* concurrent stress (6 publishers × 4 batches, 6 downloaders) with a full
  `/v1/verify` at the end; live garbage collection during serving.

`OBJSTORE_FAULT_CRASH_AT` / `OBJSTORE_FAULT_ARM_FILE` are the only knobs the
fault scenarios set; they are inert in normal operation.
