#!/usr/bin/env python3
"""Self-check for the object store: ``python3 run_tests.py`` (one command).

Every scenario talks to the service over real HTTP, through a real subprocess,
so the fault scenarios can ``SIGKILL`` the server exactly like a build node
losing power.  No third-party dependencies are used.

The runner prints a human-readable line per check and a summary, and exits
non-zero if anything fails.  ``--json`` emits a machine-readable report too.
``--filter`` runs a subset, e.g. ``--filter crash``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import sys
import threading
import time
import traceback
from typing import Callable, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from harness import (  # noqa: E402
    Runner,
    ServerProcess,
    StreamReader,
    TempDir,
    quote,
    random_bytes,
)
from fixtures import BATCHES, FAULT_POINTS  # noqa: E402

# ---------------------------------------------------------------------------
# Scenarios.  Each receives the Runner and creates its own server + data dir.
# ---------------------------------------------------------------------------

QUOTA = 1 << 20  # 1 MiB is plenty for the correctness scenarios.


def _payload(n: int, seed: int = 1) -> bytes:
    return random_bytes(n, seed=seed)


def _segments(data: bytes, size: int) -> list[tuple[int, bytes]]:
    return [(off, data[off : off + size]) for off in range(0, len(data), size)]


def scenario_basic_upload_download(r: Runner) -> None:
    """Declare, upload segments out of order, query resume point, publish, fetch."""
    with TempDir() as data_dir, ServerProcess(data_dir, QUOTA) as srv:
        cli = srv.client()
        data = _payload(7000)
        segs = _segments(data, 1000)
        state: dict = {}

        def declare_and_upload() -> str:
            up = cli.create_upload("build/artifact.bin", len(data))
            assert up["confirmed"] == 0 and up["length"] == len(data), up
            state["upload_id"] = up["upload_id"]
            # Everything except the last piece, deliberately out of order.
            for idx in [2, 0, 1, 3, 4, 5]:
                off, chunk = segs[idx]
                resp = cli.put_segment(up["upload_id"], idx, off, chunk)
                assert resp.status == 200, resp
            return "7 x 1000 B pieces accepted out of order"

        def resume_point() -> str:
            sid = state["upload_id"]
            before = cli.get(f"/v1/uploads/{sid}").json()
            assert before["confirmed"] == 6000, before
            assert [r["offset"] for r in before["confirmed_ranges"]] == [0]
            resp = cli.put_segment(sid, 6, segs[6][0], segs[6][1])
            assert resp.status == 200 and resp.json()["confirmed"] == 7000, resp
            return "resume at 6000, then confirmed 7000"

        def publish_and_fetch() -> str:
            resp = cli.complete(state["upload_id"])
            assert resp.status in (200, 201), resp
            version = resp.json()["version_id"]
            state["version"] = version
            assert resp.headers.get("x-version-id") == version
            got = cli.download("build/artifact.bin")
            assert got.status == 200 and got.body == data, got
            assert got.headers["x-version-id"] == version
            assert cli.download("build/artifact.bin", version).body == data
            assert cli.download_by_version(version).body == data
            return f"version {version[:10]}, 3 download routes agree"

        def head_request() -> str:
            resp = cli.request("HEAD", "/v1/objects/build/artifact.bin/content")
            assert resp.status == 200, resp
            assert resp.headers["content-length"] == str(len(data))
            assert resp.headers["x-version-id"] == state["version"]
            return f"HEAD advertises {len(data)} bytes, no body"

        def metadata_only() -> str:
            usage = cli.usage()
            assert usage["used_bytes"] == len(data), usage
            assert usage["metadata"]["segments"] == 7, usage
            return f"content={usage['used_bytes']} B counted, {usage['metadata']['segments']} segments in metadata"

        for title, fn in [
            ("declare length and upload segments out of order", declare_and_upload),
            ("query the confirmed resume position", resume_point),
            ("publish, then download by name and by version", publish_and_fetch),
            ("HEAD reports size and version without a body", head_request),
            ("quota accounting counts content, not metadata", metadata_only),
        ]:
            r.check(title, fn)


def scenario_segment_retry_semantics(r: Runner) -> None:
    """Retry same bytes -> OK; different bytes -> rejected; unconfirmed is hidden."""
    with TempDir() as data_dir, ServerProcess(data_dir, QUOTA) as srv:
        cli = srv.client()
        chunk_a = b"A" * 4096
        chunk_b = b"B" * 4096
        up = cli.create_upload("logs/run.log", 8192)
        sid = up["upload_id"]

        def same_content_retry() -> str:
            for _ in range(3):
                resp = cli.put_segment(sid, 0, 0, chunk_a)
                assert resp.status == 200, resp
            state = cli.get(f"/v1/uploads/{sid}").json()
            assert len(state["segments"]) == 1, state
            assert state["confirmed"] == 4096, state
            return "3 identical retries, 1 stored segment"

        def different_content_rejected() -> str:
            resp = cli.put_segment(sid, 0, 0, chunk_b)
            assert resp.status == 409, resp
            assert resp.json()["error"]["code"] == "SegmentConflict", resp.body
            # Same range, different index -> must also be rejected.
            resp2 = cli.put_segment(sid, 9, 0, chunk_b)
            assert resp2.status == 409, resp2
            return "409 SegmentConflict"

        def unpublished_invisible() -> str:
            resp = cli.download("logs/run.log")
            assert resp.status == 404, resp
            resp = cli.complete(sid)
            assert resp.status == 409, resp  # not fully received
            assert resp.json()["error"]["code"] == "Incomplete"
            return "404 for downloads, 409 for publish"

        def bad_digest_rejected() -> str:
            resp = cli.request(
                "PUT", f"/v1/uploads/{sid}/segments/1?offset=4096", chunk_b,
                {"X-Chunk-SHA256": hashlib.sha256(b"nope").hexdigest()},
            )
            assert resp.status == 400, resp
            return "400 BadDigest"

        for name, fn in [
            ("same segment retried with identical bytes is accepted", same_content_retry),
            ("same segment retried with different bytes is rejected", different_content_rejected),
            ("unpublished content is invisible and unpublished upload cannot publish", unpublished_invisible),
            ("segment whose digest does not match its body is rejected", bad_digest_rejected),
        ]:
            r.check(name, fn)


def scenario_versions(r: Runner) -> None:
    """Each publish gets its own version; name follows the latest; ids stay pinned.

    Uses the bundled batches from ``fixtures.py``: one name, three generations,
    multi-segment transfers.
    """
    with TempDir() as data_dir, ServerProcess(data_dir, QUOTA) as srv:
        cli = srv.client()

        versions = []
        payloads = {}
        for batch in BATCHES:
            data = batch.data
            up = cli.create_upload(batch.name, len(data))
            for idx, (off, chunk) in enumerate(batch.pieces):
                resp = cli.put_segment(up["upload_id"], idx, off, chunk)
                assert resp.status == 200, resp
            resp = cli.complete(up["upload_id"])
            assert resp.status == 201, resp
            versions.append(resp.json()["version_id"])
            payloads[versions[-1]] = data

        def distinct_ids() -> str:
            assert len(set(versions)) == 3, versions
            return "3 distinct ids"

        name = BATCHES[0].name

        def name_is_latest() -> str:
            got = cli.download(name)
            assert got.headers["x-version-id"] == versions[-1], got.headers
            assert got.body == payloads[versions[-1]]
            return versions[-1][:10]

        def old_versions_downloadable() -> str:
            for vid in versions:
                got = cli.download(name, vid)
                assert got.status == 200 and got.body == payloads[vid], vid
            return "all 3 by id"

        def scoped_metadata() -> str:
            right = cli.get(f"/v1/objects/{quote(name)}/versions/{versions[0]}")
            assert right.status == 200 and right.json()["version_id"] == versions[0]
            wrong = cli.get(f"/v1/objects/other/name/versions/{versions[0]}")
            assert wrong.status == 404, wrong
            assert cli.get(f"/v1/versions/{versions[0]}").status == 200
            return "name+id scoped, bare id global"

        def listing() -> str:
            listed = cli.get(f"/v1/objects/{quote(name)}/versions").json()["versions"]
            assert [v["version_id"] for v in listed] == list(reversed(versions)), listed
            assert listed[0]["active"] and not listed[1]["active"]
            # Older versions are superseded, not retired: still fetchable.
            assert all(v["retired_at"] is None for v in listed)
            return f"{len(listed)} versions listed newest first"

        for title, fn in [
            ("every successful publish generates a distinct version", distinct_ids),
            ("download by name resolves the last effective version", name_is_latest),
            ("older versions remain downloadable by version id", old_versions_downloadable),
            ("version metadata is scoped by name when namespaced", scoped_metadata),
            ("version listing is newest-first with one active", listing),
        ]:
            r.check(title, fn)


def scenario_lost_response(r: Runner) -> None:
    """The completion committed but its response was lost: retries return it."""
    with TempDir() as data_dir:
        # Fault: the process dies after the publish transaction commits, before
        # the HTTP response is written.
        srv = ServerProcess(
            data_dir, QUOTA, env={"OBJSTORE_FAULT_CRASH_AT": "complete_after_commit"}
        ).start()
        cli = srv.client()
        data = _payload(5000, seed=7)
        up = cli.create_upload("ci/out.bin", len(data))
        cli.put_segment(up["upload_id"], 0, 0, data)
        sid = up["upload_id"]
        try:
            cli.complete(sid)
        except (ConnectionError, OSError, Exception):  # noqa: BLE001
            pass
        srv.kill()
        srv.start()  # restart on the same data directory
        cli = srv.client()

        def retry_returns_original() -> str:
            assert "complete_after_commit" in FAULT_POINTS
            first = cli.complete(sid)
            assert first.status == 200, first
            version = first.json()["version_id"]
            again = cli.complete(sid)
            assert again.status == 200, again
            assert again.json()["version_id"] == version, (again.json(), version)
            got = cli.download("ci/out.bin")
            assert got.body == data and got.headers["x-version-id"] == version
            # And a keyed retry against a fresh client behaves identically.
            keyed = cli.complete(sid, key="build-42-publish")
            assert keyed.json()["version_id"] == version
            return version[:10]

        r.check("publish survives a lost response and a forced restart", retry_returns_original)
        srv.kill()


def scenario_idempotency_key(r: Runner) -> None:
    """A client-chosen idempotency key maps retries to the first version."""
    with TempDir() as data_dir, ServerProcess(data_dir, QUOTA) as srv:
        cli = srv.client()

        def keyed() -> str:
            data = _payload(2048, seed=9)
            up = cli.create_upload("deploy/a", len(data))
            cli.put_segment(up["upload_id"], 0, 0, data)
            one = cli.complete(up["upload_id"], key="job-7")
            two = cli.complete(up["upload_id"], key="job-7")
            assert one.json()["version_id"] == two.json()["version_id"]
            # The same key replayed against a *different* upload is a conflict,
            # never a silent alias of somebody else's content.
            up2 = cli.create_upload("deploy/a", len(data))
            cli.put_segment(up2["upload_id"], 0, 0, data)
            three = cli.complete(up2["upload_id"], key="job-7")
            assert three.status == 409, three
            assert three.json()["error"]["code"] == "IdempotencyConflict"
            # Completion of up2 with its own key still works and is distinct.
            four = cli.complete(up2["upload_id"], key="job-8")
            assert four.status == 201
            assert four.json()["version_id"] != one.json()["version_id"]
            return one.json()["version_id"][:10]

        r.check("idempotency key returns the original version", keyed)


def scenario_crash_recovery(r: Runner) -> None:
    """SIGKILL with confirmed segments on disk: resume, publish, download intact."""
    with TempDir() as data_dir:
        srv = ServerProcess(data_dir, QUOTA).start()
        cli = srv.client()
        data = _payload(64 * 1024, seed=21)
        segs = _segments(data, 4096)
        up = cli.create_upload("big/artifact.tar", len(data))
        sid = up["upload_id"]
        for idx in range(8):  # first third confirmed and acknowledged
            resp = cli.put_segment(sid, idx, segs[idx][0], segs[idx][1])
            assert resp.status == 200, resp
        srv.kill(9)
        srv.start()
        cli = srv.client()

        def resume_after_kill() -> str:
            state = cli.get(f"/v1/uploads/{sid}").json()
            assert state["confirmed"] == 8 * 4096, state
            for idx in range(8, len(segs)):
                resp = cli.put_segment(sid, idx, segs[idx][0], segs[idx][1])
                assert resp.status == 200, resp
            resp = cli.complete(sid)
            assert resp.status == 201, resp
            got = cli.download("big/artifact.tar")
            assert got.body == data, f"corrupt download ({len(got.body)} bytes)"
            usage = cli.usage()
            assert usage["used_bytes"] == len(data), usage
            report = cli.get("/v1/verify").json()
            assert report["ok"], report
            return f"resumed at {8*4096}, final size {len(data)}"

        r.check("confirmed segments survive SIGKILL and upload resumes", resume_after_kill)

        srv.kill(9)
        srv.start()
        cli = srv.client()

        def published_survives() -> str:
            got = cli.download("big/artifact.tar")
            assert got.status == 200 and got.body == data
            versions = cli.get(f"/v1/objects/{quote('big/artifact.tar')}/versions").json()["versions"]
            assert len(versions) == 1 and versions[0]["active"]
            return versions[0]["version_id"][:10]

        r.check("published version survives a second SIGKILL", published_survives)
        srv.kill()


def scenario_crash_between_fsync_and_commit(r: Runner) -> None:
    """A kill after the bytes hit disk but before the manifest: bytes are not
    counted, not visible, and the retried segment still succeeds."""
    with TempDir() as data_dir:
        arm = os.path.join(data_dir, "arm-fault")
        srv = ServerProcess(
            data_dir,
            QUOTA,
            env={"OBJSTORE_FAULT_CRASH_AT": "segment_after_fsync", "OBJSTORE_FAULT_ARM_FILE": arm},
        ).start()
        cli = srv.client()
        data = _payload(9000, seed=33)
        up = cli.create_upload("atomic/x.bin", len(data))
        sid = up["upload_id"]
        cli.put_segment(sid, 0, 0, data[:4096])  # normal, unarmed
        open(arm, "w").close()  # arm: the next segment dies after fsync
        try:
            cli.put_segment(sid, 1, 4096, data[4096:8192])
        except Exception:  # noqa: BLE001
            pass
        srv.kill()
        srv.start()
        cli = srv.client()

        def verified() -> str:
            state = cli.get(f"/v1/uploads/{sid}").json()
            assert state["confirmed"] == 4096, state
            usage = cli.usage()
            assert usage["used_bytes"] == 4096, usage
            assert usage["metadata"]["content_files"] == 1, usage
            resp = cli.complete(sid)
            assert resp.status == 409 and resp.json()["error"]["code"] == "Incomplete", resp
            for idx in (1, 2):
                off = idx * 4096
                resp = cli.put_segment(sid, idx, off, data[off : off + 4096])
                assert resp.status == 200, resp
            assert cli.complete(sid).status == 201
            assert cli.download("atomic/x.bin").body == data
            report = cli.get("/v1/verify").json()
            assert report["ok"], report
            return "uncommitted bytes were not counted nor exposed"

        r.check("kill between fsync and manifest commit is harmless", verified)
        srv.kill()


def scenario_retire_and_quota(r: Runner) -> None:
    """Retired content keeps the promise to active readers, then is reclaimed."""
    quota = 8 << 20
    with TempDir() as data_dir, ServerProcess(data_dir, quota) as srv:
        cli = srv.client()
        # Large enough that a paused reader really blocks the server mid-body
        # instead of the whole response fitting into socket buffers.
        data = _payload(2 << 20, seed=55)

        def fill_and_retire() -> str:
            up = cli.create_upload("release/one.bin", len(data))
            cli.put_segment(up["upload_id"], 0, 0, data)
            v1 = cli.complete(up["upload_id"]).json()["version_id"]

            # Retire the only version: downloads of it must now be refused.
            resp = cli.request("DELETE", f"/v1/objects/{quote('release/one.bin')}/versions/{v1}")
            assert resp.status == 204, resp
            resp = cli.download("release/one.bin")
            assert resp.status == 404, resp
            resp = cli.download("release/one.bin", v1)
            assert resp.status == 410, resp
            assert resp.json()["error"]["code"] == "Gone"
            usage = cli.usage()
            assert usage["used_bytes"] == 0, usage  # nobody was reading: reclaimed at once
            return "reclaimed immediately with no readers"

        r.check("retired version is refused and reclaimed", fill_and_retire)

        def open_download_survives_retire() -> str:
            up = cli.create_upload("release/two.bin", len(data))
            cli.put_segment(up["upload_id"], 0, 0, data)
            v1 = cli.complete(up["upload_id"]).json()["version_id"]
            reader = StreamReader(srv.base, f"/v1/objects/{quote('release/two.bin')}/content/{v1}")
            assert reader.status == 200, reader.status
            first = reader.read(20_000)
            assert len(first) == 20_000
            # While the reader is paused mid-body, retire it and publish a new one.
            resp = cli.request("DELETE", f"/v1/objects/{quote('release/two.bin')}/versions/{v1}")
            assert resp.status == 204, resp
            assert cli.download("release/two.bin", v1).status == 410
            usage = cli.usage()
            assert usage["used_bytes"] == len(data), usage  # pinned, not yet reclaimed
            rest = reader.read_all(expected=len(data) - 20_000)
            assert first + rest == data, "reader lost bytes after retirement"
            reader.close()
            deadline = time.time() + 5
            while time.time() < deadline:
                if cli.usage()["used_bytes"] == 0:
                    break
                time.sleep(0.02)
            usage = cli.usage()
            assert usage["used_bytes"] == 0, usage
            return "paused reader finished, then bytes were reclaimed"

        r.check("paused download completes and only then is content reclaimed",
                open_download_survives_retire)
        srv.kill()


def scenario_quota(r: Runner) -> None:
    """Quota covers staged and retained bytes, never metadata; publish-at-full works."""
    quota = 40_000
    with TempDir() as data_dir, ServerProcess(data_dir, quota) as srv:
        cli = srv.client()
        a = _payload(30_000, seed=77)
        b = _payload(20_000, seed=78)

        staged_id = ""

        def staged_counts() -> str:
            nonlocal staged_id
            up = cli.create_upload("q/a", len(a))
            resp = cli.put_segment(up["upload_id"], 0, 0, a)
            assert resp.status == 200, resp
            staged_id = up["upload_id"]
            usage = cli.usage()
            assert usage["staged_bytes"] == 30_000, usage
            assert usage["used_bytes"] == 30_000
            return "30 kB staged of 40 kB"

        def over_quota_rejected() -> str:
            up = cli.create_upload("q/b", len(b))
            resp = cli.put_segment(up["upload_id"], 0, 0, b)
            assert resp.status == 507, resp
            assert resp.json()["error"]["code"] == "QuotaExceeded"
            return "507 QuotaExceeded"

        def publish_at_full() -> str:
            # Fill to the brim with two 20 kB uploads, then publish at 100% used.
            up = cli.create_upload("q/fill", 10_000)
            resp = cli.put_segment(up["upload_id"], 0, 0, _payload(10_000, seed=5))
            assert resp.status == 200, resp
            usage = cli.usage()
            assert usage["used_bytes"] == quota, usage
            fill_id = up["upload_id"]
            resp = cli.complete(fill_id)
            assert resp.status == 201, resp
            got = cli.download("q/fill")
            assert got.status == 200 and len(got.body) == 10_000
            return "published with used == quota"

        def concurrent_quota_race() -> str:
            # A dedicated server: 100 kB of room, ten uploads of 30 kB each race
            # for it.  Quota is charged inside the write transaction, so exactly
            # three fit and the other seven must be refused -- never a byte over.
            race_quota = 100_000
            with TempDir() as race_dir, ServerProcess(race_dir, race_quota) as race_srv:
                results: list[int] = []
                lock = threading.Lock()

                def publish(i: int) -> None:
                    cli2 = race_srv.client()
                    up = cli2.create_upload(f"q/race{i}", 30_000)
                    resp = cli2.put_segment(up["upload_id"], 0, 0, _payload(30_000, seed=900 + i))
                    with lock:
                        results.append(resp.status)

                threads = [threading.Thread(target=publish, args=(i,)) for i in range(10)]
                for t in threads:
                    t.start()
                for t in threads:
                    t.join(30)
                assert len(results) == 10, results
                assert sorted(results) == [200] * 3 + [507] * 7, results
                usage = race_srv.client().usage()
                assert usage["used_bytes"] == 90_000, usage
                assert race_srv.client().get("/v1/verify").json()["ok"]
            return "10 racers for 100 kB: 3 accepted, 7 refused, never over quota"

        def metadata_not_charged() -> str:
            usage = cli.usage()
            assert usage["used_bytes"] == quota, usage
            assert usage["metadata"]["db_bytes"] > 0, usage
            assert usage["metadata"]["segments"] > 0, usage
            return f"metadata db={usage['metadata']['db_bytes']} B excluded"

        def abort_frees_quota() -> str:
            # The 30 kB staged upload from the first check is still open; abort
            # it and prove its bytes are released immediately.
            assert cli.usage()["staged_bytes"] == 30_000
            assert cli.request("DELETE", f"/v1/uploads/{staged_id}").status == 204
            assert cli.usage()["used_bytes"] == quota - 30_000
            # Now fill the whole quota with one staged upload, then abort it.
            up = cli.create_upload("q/tmp", quota - 10_000)
            resp = cli.put_segment(up["upload_id"], 0, 0, _payload(quota - 10_000, seed=6))
            assert resp.status == 200, resp
            assert cli.usage()["used_bytes"] == quota
            assert cli.request("DELETE", f"/v1/uploads/{up['upload_id']}").status == 204
            assert cli.usage()["used_bytes"] == 10_000  # only the published fill remains
            return "aborted uploads release their reservation"

        for name, fn in [
            ("staged bytes count against the quota", staged_counts),
            ("upload beyond the quota is refused", over_quota_rejected),
            ("a fully received upload publishes at a completely full quota", publish_at_full),
            ("metadata is accounted separately from content", metadata_not_charged),
            ("concurrent uploads never overshoot the quota", concurrent_quota_race),
            ("aborting an upload frees its reservation", abort_frees_quota),
        ]:
            r.check(name, fn)
        srv.kill()


def scenario_download_while_publishing(r: Runner) -> None:
    """A download picks its version at start; publishers never block it."""
    with TempDir() as data_dir, ServerProcess(data_dir, 4 << 20) as srv:
        cli = srv.client()
        size = 1 << 20
        v1_data = _payload(size, seed=1)
        v2_data = _payload(size, seed=2)

        up = cli.create_upload("stream/live.bin", size)
        for idx, (off, chunk) in enumerate(_segments(v1_data, 65536)):
            assert cli.put_segment(up["upload_id"], idx, off, chunk).status == 200
        v1 = cli.complete(up["upload_id"]).json()["version_id"]

        reader = StreamReader(srv.base, f"/v1/objects/stream/live.bin/content")
        assert reader.status == 200 and reader.version_id == v1
        head = reader.read(64 * 1024)

        def publish_during_download() -> str:
            up2 = cli.create_upload("stream/live.bin", size)
            for idx, (off, chunk) in enumerate(_segments(v2_data, 65536)):
                assert cli.put_segment(up2["upload_id"], idx, off, chunk).status == 200
            started = time.time()
            resp = cli.complete(up2["upload_id"])
            elapsed = time.time() - started
            assert resp.status == 201, resp
            v2 = resp.json()["version_id"]
            assert v2 != v1
            assert cli.download("stream/live.bin").body == v2_data
            assert elapsed < 10, f"publish blocked for {elapsed:.2f}s"
            return f"published v2 in {elapsed*1000:.0f} ms while v1 was open"

        r.check("publish and download proceed concurrently", publish_during_download)

        def pinned_reader_sees_old_version() -> str:
            rest = reader.read_all(expected=size - 64 * 1024)
            assert head + rest == v1_data, "reader mixed versions"
            reader.close()
            return "reader finished on the version it started with"

        r.check("a download keeps the version chosen at its start", pinned_reader_sees_old_version)
        srv.kill()


def scenario_concurrent_stress(r: Runner) -> None:
    """Concurrent publishers + downloaders + retirees; no corruption, no leak."""
    with TempDir() as data_dir, ServerProcess(data_dir, 8 << 20) as srv:
        names = [f"stress/n{i}" for i in range(4)]
        published: dict[str, list[tuple[str, bytes]]] = {n: [] for n in names}
        published_lock = threading.Lock()
        errors: list[str] = []
        stop = threading.Event()

        def publisher(worker: int) -> None:
            cli = srv.client()
            rng = random.Random(worker)
            for round_no in range(6):
                name = names[rng.randrange(len(names))]
                size = rng.randrange(20_000, 120_000)
                data = random_bytes(size, seed=worker * 100 + round_no)
                if stop.is_set():
                    return
                try:
                    up = cli.create_upload(name, size)
                    for idx, (off, chunk) in enumerate(_segments(data, 8192)):
                        resp = cli.put_segment(up["upload_id"], idx, off, chunk)
                        if resp.status != 200:
                            raise AssertionError(f"segment {idx}: {resp.status} {resp.body[:200]}")
                    resp = cli.complete(up["upload_id"])
                    if resp.status != 201:
                        raise AssertionError(f"complete: {resp.status} {resp.body[:200]}")
                    with published_lock:
                        published[name].append((resp.json()["version_id"], data))
                except Exception as err:  # noqa: BLE001
                    errors.append(f"publisher{worker}: {type(err).__name__}: {err}")
                    return

        def downloader(worker: int) -> None:
            cli = srv.client(timeout=60)
            rng = random.Random(1000 + worker)
            for _ in range(15):
                if stop.is_set():
                    return
                name = names[rng.randrange(len(names))]
                try:
                    resp = cli.download(name)
                    if resp.status == 404:
                        time.sleep(0.01)
                        continue
                    if resp.status != 200:
                        raise AssertionError(f"{name}: {resp.status}")
                    with published_lock:
                        candidates = [(v, d) for v, d in published[name] if len(d) == len(resp.body)]
                    if candidates and all(d != resp.body for _, d in candidates):
                        raise AssertionError(f"{name}: downloaded bytes match no published version")
                except Exception as err:  # noqa: BLE001
                    errors.append(f"downloader{worker}: {type(err).__name__}: {err}")
                    return

        threads = [threading.Thread(target=publisher, args=(i,)) for i in range(4)]
        threads += [threading.Thread(target=downloader, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=90)
        stop.set()
        alive = [t for t in threads if t.is_alive()]
        assert not alive, f"{len(alive)} threads hung"
        assert not errors, "; ".join(errors[:5])

        def final_state() -> str:
            cli = srv.client()
            for name in names:
                with published_lock:
                    entries = published[name]
                if not entries:
                    continue
                latest_version, latest_data = entries[-1]
                got = cli.download(name)
                assert got.status == 200, got
                assert got.body == latest_data, f"{name}: name no longer resolves to latest"
                for version, data in entries:
                    if version == latest_version:
                        continue
                    one = cli.download(name, version)
                    assert one.status == 200, (name, version, one.status)
                    assert one.body == data
            usage = cli.usage()
            assert usage["open_downloads"] == 0, usage
            return f"{sum(len(v) for v in published.values())} versions intact, used={usage['used_bytes']}"

        r.check("concurrent uploads and downloads do not corrupt versions", final_state)
        srv.kill()


def scenario_reclaim_during_download_stress(r: Runner) -> None:
    """Downloads racing take-downs: every started download finishes correctly,
    and quota returns to a stable value once they drain."""
    with TempDir() as data_dir, ServerProcess(data_dir, 16 << 20) as srv:
        cli = srv.client()
        size = 2 << 20
        payloads: dict[str, bytes] = {}
        readers: list[tuple[StreamReader, str, str]] = []
        errors: list[str] = []

        for round_no in range(4):
            data = _payload(size, seed=500 + round_no)
            up = cli.create_upload("reclaim/obj", size)
            for idx, (off, chunk) in enumerate(_segments(data, 262_144)):
                assert cli.put_segment(up["upload_id"], idx, off, chunk).status == 200
            vid = cli.complete(up["upload_id"]).json()["version_id"]
            payloads[vid] = data
            reader = StreamReader(srv.base, f"/v1/objects/reclaim/obj/content/{vid}")
            assert reader.status == 200
            assert len(reader.read(50_000)) == 50_000
            readers.append((reader, "reclaim/obj", vid))

        def retire_all_while_reading() -> str:
            resp = cli.request("DELETE", f"/v1/objects/{quote('reclaim/obj')}/versions")
            assert resp.status == 200, resp
            body = resp.json()
            assert body["retired"] == 4, body
            usage = cli.usage()
            assert usage["used_bytes"] == 4 * size, usage  # all four are pinned
            assert usage["open_downloads"] == 4, usage
            return "4 retired, 4 readers still hold the bytes"

        r.check("take-down while four downloads are paused keeps their bytes",
                retire_all_while_reading)

        def readers_finish() -> str:
            for reader, name, vid in readers:
                rest = reader.read_all(expected=size - 50_000)
                assert rest, "empty tail"
            for reader, name, vid in readers:
                reader.close()
            deadline = time.time() + 10
            while time.time() < deadline:
                if cli.usage()["used_bytes"] == 0:
                    break
                time.sleep(0.02)
            usage = cli.usage()
            assert usage["used_bytes"] == 0, usage
            assert usage["open_downloads"] == 0, usage
            return "4 readers finished, quota fully reclaimed"

        r.check("downloads outlive retirement and bytes are reclaimed at the end", readers_finish)

        def refused_after() -> str:
            for vid in payloads:
                resp = cli.download("reclaim/obj", vid)
                assert resp.status == 410, (vid, resp.status)
            assert cli.download("reclaim/obj").status == 404
            return "410 for old ids, 404 for the name"

        r.check("retired versions are refused for new downloads", refused_after)
        srv.kill()


def scenario_gc_and_disk(r: Runner) -> None:
    """Cleanup runs while serving; abandoned uploads free disk space."""
    with TempDir() as data_dir, ServerProcess(data_dir, QUOTA) as srv:
        cli = srv.client()

        def janitor_runs_live() -> str:
            data = _payload(50_000, seed=11)
            up = cli.create_upload("gc/stale.bin", len(data))
            cli.put_segment(up["upload_id"], 0, 0, data)
            assert cli.usage()["used_bytes"] == len(data)
            resp = cli.post_json("/v1/gc", {"force": True})
            assert resp.status == 200, resp
            result = resp.json()
            assert result["reclaimed_uploads"] >= 1, result
            assert cli.usage()["used_bytes"] == 0, cli.usage()
            assert cli.get("/healthz").status == 200  # still serving
            return f"reclaimed {result['reclaimed_bytes']} B while serving"

        r.check("garbage collection reclaims abandoned uploads while serving", janitor_runs_live)

        def restart_sees_clean_state() -> str:
            for _ in range(3):
                up = cli.create_upload("gc/keep", 1000)
                cli.put_segment(up["upload_id"], 0, 0, b"x" * 1000)
            cli.complete(up["upload_id"])
            return "ok"

        r.check("small uploads publish normally", restart_sees_clean_state)
        srv.kill()


SCENARIOS: list[tuple[str, Callable[[Runner], None]]] = [
    ("basic upload, resume point, publish, download", scenario_basic_upload_download),
    ("segment retry semantics", scenario_segment_retry_semantics),
    ("versioned publishes", scenario_versions),
    ("lost completion response", scenario_lost_response),
    ("idempotency key", scenario_idempotency_key),
    ("forced restart recovery", scenario_crash_recovery),
    ("kill between fsync and commit", scenario_crash_between_fsync_and_commit),
    ("retirement and quota", scenario_retire_and_quota),
    ("content quota", scenario_quota),
    ("download while publishing", scenario_download_while_publishing),
    ("concurrent stress", scenario_concurrent_stress),
    ("reclaim racing downloads", scenario_reclaim_during_download_stress),
    ("live garbage collection", scenario_gc_and_disk),
]


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run the object store's self-checks.")
    parser.add_argument("--filter", default=None, help="only run scenarios whose name contains this")
    parser.add_argument("--json", default=None, metavar="PATH", help="write a JSON report here")
    parser.add_argument("--quiet", action="store_true", help="one line per scenario")
    args = parser.parse_args(argv)

    started = time.time()
    runner = Runner(verbose=not args.quiet)
    print("object store self-check")
    print("=" * 72)
    for title, fn in SCENARIOS:
        if args.filter and args.filter.lower() not in title.lower():
            continue
        print(f"\n{title}")
        t0 = time.time()
        try:
            fn(runner)
        except Exception as err:  # noqa: BLE001
            runner.result.add(f"{title}: scenario aborted", False, f"{type(err).__name__}: {err}", time.time() - t0)
            if not args.quiet:
                traceback.print_exc()
        if args.quiet:
            elapsed = time.time() - t0
            bad = [c for c in runner.result.checks if not c.ok]
            print(f"  {'ok' if not bad else 'FAILED'}  ({elapsed:.1f}s)")
    total_time = time.time() - started

    result = runner.result
    print("\n" + "=" * 72)
    failed = result.failed
    print(f"{len(result.checks) - len(failed)} passed, {len(failed)} failed in {total_time:.1f}s")
    if failed:
        print("\nfailures:")
        for check in failed:
            print(f"  - {check.name}: {check.detail}")
    if args.json:
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump(
                {
                    "ok": not failed,
                    "duration_s": round(total_time, 2),
                    "checks": [
                        {"name": c.name, "ok": c.ok, "detail": c.detail, "duration_s": round(c.duration, 3)}
                        for c in result.checks
                    ],
                },
                fh,
                indent=2,
            )
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
