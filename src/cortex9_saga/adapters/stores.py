"""Event stores: in-memory and JSON files (one file per saga)."""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import re
from collections.abc import Iterator, Sequence
from pathlib import Path
from threading import Lock

from cortex9_saga.adapters.codec import from_dict, to_dict
from cortex9_saga.domain.events import Event
from cortex9_saga.ports import ConcurrencyError

_SAFE_ID = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")


class InMemorySagaStore:
    def __init__(self) -> None:
        self._streams: dict[str, list[Event]] = {}
        self._lock = Lock()

    def append(self, saga_id: str, expected_version: int, events: Sequence[Event]) -> None:
        with self._lock:
            stream = self._streams.setdefault(saga_id, [])
            if len(stream) != expected_version:
                raise ConcurrencyError(
                    f"{saga_id}: expected version {expected_version}, found {len(stream)}"
                )
            stream.extend(events)

    def load(self, saga_id: str) -> Sequence[Event]:
        with self._lock:
            return list(self._streams.get(saga_id, []))

    def list_ids(self) -> Sequence[str]:
        with self._lock:
            return sorted(self._streams)


class JsonFileSagaStore:
    """Durable store: ``<dir>/<saga_id>.json`` holds ``{"version": n, "events": [...]}``.

    Appends take an exclusive ``flock`` on a per-saga lock file, verify the
    expected version, then replace the file atomically (write temp, fsync,
    rename, fsync directory). A crash leaves either the old or the new file.
    """

    def __init__(self, directory: str | os.PathLike[str]) -> None:
        self._dir = Path(directory)
        self._dir.mkdir(parents=True, exist_ok=True)

    def _path(self, saga_id: str) -> Path:
        if not _SAFE_ID.match(saga_id):
            raise ValueError(f"saga id not safe for a file name: {saga_id!r}")
        return self._dir / f"{saga_id}.json"

    @contextlib.contextmanager
    def _locked(self, saga_id: str) -> Iterator[None]:
        lock_path = self._dir / f".{saga_id}.lock"
        with open(lock_path, "a+") as fh:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)

    def _read(self, path: Path) -> list[Event]:
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return []
        events = [from_dict(d) for d in raw["events"]]
        if raw.get("version") != len(events):
            raise ValueError(f"{path}: version field does not match event count")
        return events

    def append(self, saga_id: str, expected_version: int, events: Sequence[Event]) -> None:
        path = self._path(saga_id)
        with self._locked(saga_id):
            current = self._read(path)
            if len(current) != expected_version:
                raise ConcurrencyError(
                    f"{saga_id}: expected version {expected_version}, found {len(current)}"
                )
            all_events = current + list(events)
            doc = {"version": len(all_events), "events": [to_dict(e) for e in all_events]}
            tmp = path.with_suffix(".json.tmp")
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(doc, fh, separators=(",", ":"), sort_keys=True)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, path)
            dir_fd = os.open(self._dir, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)

    def load(self, saga_id: str) -> Sequence[Event]:
        return self._read(self._path(saga_id))

    def list_ids(self) -> Sequence[str]:
        return sorted(p.stem for p in self._dir.glob("*.json"))
