"""Bundled test data and fault fixtures.

Test data
---------
:func:`sample_artifact` builds a deterministic, pseudo-random payload without
touching the network, and :data:`BATCHES` describes three "build batches" of the
same artifact name -- exactly the workload the storage was designed for: one
name, many publish generations, retried transfers.

Fault fixtures
--------------
The service can be told to die at a precise point of its commit protocol, from
the outside, with two environment variables (see :data:`FAULT_POINTS`).  They are
inert unless set and are only ever used by the test suite.

``OBJSTORE_FAULT_CRASH_AT``   name of the fault point to trip.
``OBJSTORE_FAULT_ARM_FILE``   optional path; the fault only trips once this file
                              exists, and disarms itself first.  Lets a scenario
                              run ordinary traffic and then kill the next call.
"""

from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass


def sample_artifact(size: int, seed: int) -> bytes:
    """Deterministic pseudo-random bytes; identical on every machine/Python."""
    rng = random.Random(seed)
    return rng.randbytes(size)


def segments(data: bytes, chunk: int) -> list[tuple[int, bytes]]:
    return [(off, data[off : off + chunk]) for off in range(0, len(data), chunk)]


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@dataclass(frozen=True)
class Batch:
    name: str
    size: int
    seed: int
    chunk: int

    @property
    def data(self) -> bytes:
        return sample_artifact(self.size, self.seed)

    @property
    def pieces(self) -> list[tuple[int, bytes]]:
        return segments(self.data, self.chunk)


# Three batches of one and the same artifact name.  Sizes differ so a download
# that accidentally mixed generations is immediately detectable.
BATCHES: tuple[Batch, ...] = (
    Batch("nightly/build.tar", size=180_000, seed=101, chunk=16_384),
    Batch("nightly/build.tar", size=220_000, seed=102, chunk=16_384),
    Batch("nightly/build.tar", size=160_000, seed=103, chunk=16_384),
)

# A second name with small artifacts, handy for quota tests.
SMALL: tuple[Batch, ...] = (
    Batch("ci/logs.txt", size=9_000, seed=201, chunk=1_000),
    Batch("ci/logs.txt", size=12_000, seed=202, chunk=1_000),
)

FAULT_POINTS: dict[str, str] = {
    "segment_after_fsync": (
        "the bytes of a segment are fsync-ed but the manifest transaction has not "
        "committed; after restart the segment must be absent from the resume point, "
        "uncounted by the quota, and acceptable to retry with the same bytes"
    ),
    "complete_after_commit": (
        "the publish transaction has committed but no HTTP response was written; a "
        "restarted server must return the original version id when the client retries"
    ),
}
