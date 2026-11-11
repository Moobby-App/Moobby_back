"""Opt-in bounded R01 sink. Filesystem operations belong to its single writer."""
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import fcntl
import json
import math
import os
from pathlib import Path
from queue import Empty, Full, Queue
import stat
from threading import Event, Lock, Thread
from uuid import UUID

from mooby.api.observability import _CATEGORIES, _EVENTS, _METHODS, _ROUTES, _STAGES

ROOT = Path(__file__).resolve().parents[3]
MAX_RECORD_BYTES = 4096
EXCEPTIONS = frozenset({"configuration_error", "connection_error", "database_error",
    "validation_error", "mapping_error", "dependency_error", "transport_error",
    "http_error", "unexpected_error", "cancellation"})
FIELDS = frozenset({"event", "request_id", "timestamp", "route", "method", "status",
    "category", "duration_ms", "failure_stage", "exception_category", "response_started",
    "disconnect_observed", "transport_aborted", "transport_exception_category"})


@dataclass(frozen=True)
class SinkSettings:
    capacity: int = 1024
    max_bytes: int = 1_000_000
    backups: int = 3
    flush_seconds: float = 0.5

    def __post_init__(self):
        for value, low, high in ((self.capacity, 1, 8192),
                                 (self.max_bytes, MAX_RECORD_BYTES, 100_000_000),
                                 (self.backups, 1, 10)):
            if type(value) is not int or not low <= value <= high:
                raise ValueError("invalid diagnostic limits")
        if (type(self.flush_seconds) not in (float, int)
            or not math.isfinite(self.flush_seconds) or not 0 <= self.flush_seconds <= 2):
            raise ValueError("invalid diagnostic flush limit")

    @classmethod
    def from_environment(cls):
        return cls(int(os.environ.get("MOOBY_DIAGNOSTIC_CAPACITY", "1024")),
                   int(os.environ.get("MOOBY_DIAGNOSTIC_MAX_BYTES", "1000000")),
                   int(os.environ.get("MOOBY_DIAGNOSTIC_BACKUPS", "3")),
                   float(os.environ.get("MOOBY_DIAGNOSTIC_FLUSH_SECONDS", "0.5")))


def encode_record(record):
    """Reject unknown fields/types/values; never stringify arbitrary objects."""
    if type(record) is not dict or len(record) not in {len(FIELDS), len(FIELDS) + 2}:
        raise ValueError("invalid diagnostic record")
    expected = FIELDS | ({"stage", "stage_duration_ms"} if record.get("event") == "request_stage" else set())
    if set(record) != expected:
        raise ValueError("invalid diagnostic fields")
    value = record.copy()
    for key, choices, nullable in (("event", _EVENTS, False), ("route", _ROUTES, True),
        ("method", _METHODS | {"OTHER"}, False), ("category", _CATEGORIES, True),
        ("failure_stage", _STAGES, True), ("exception_category", EXCEPTIONS, True),
        ("transport_exception_category", EXCEPTIONS, True)):
        item = value[key]
        if item is None and nullable:
            continue
        if type(item) is not str or item not in choices:
            raise ValueError("invalid diagnostic value")
    request_id = value["request_id"]
    if type(request_id) is not str or len(request_id) != 32:
        raise ValueError("invalid correlation")
    parsed = UUID(hex=request_id)
    if parsed.version != 4 or parsed.hex != request_id:
        raise ValueError("invalid correlation")
    timestamp = value["timestamp"]
    if type(timestamp) is not str or len(timestamp) > 40:
        raise ValueError("invalid timestamp")
    parsed_time = datetime.fromisoformat(timestamp)
    if parsed_time.tzinfo is None or parsed_time.utcoffset().total_seconds() != 0:
        raise ValueError("invalid timestamp")
    value["timestamp"] = parsed_time.astimezone(timezone.utc).isoformat()
    if value["status"] is not None and (type(value["status"]) is not int or not 100 <= value["status"] <= 599):
        raise ValueError("invalid status")
    for key in ("response_started", "disconnect_observed", "transport_aborted"):
        if type(value[key]) is not bool:
            raise ValueError("invalid flag")
    for key in ("duration_ms", "stage_duration_ms"):
        if key in value:
            item = value[key]
            if type(item) not in (float, int) or not math.isfinite(item) or not 0 <= item <= 1e12:
                raise ValueError("invalid duration")
    if "stage" in value and (type(value["stage"]) is not str or value["stage"] not in _STAGES):
        raise ValueError("invalid stage")
    payload = (json.dumps(value, separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")
    if len(payload) > MAX_RECORD_BYTES:
        raise ValueError("oversized record")
    return payload


class _FileWriter:
    """Fixed relative location, descriptor-relative operations, no symlink following."""
    name = "api-requests.jsonl"

    def __init__(self, root, settings):
        self.settings = settings
        self.directory = self.file = self.lockfile = None
        try:
            self.directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            for component in (".local", "services", "diagnostics"):
                try:
                    os.mkdir(component, 0o700, dir_fd=self.directory)
                except FileExistsError:
                    pass
                child = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                dir_fd=self.directory)
                info = os.fstat(child)
                if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o022:
                    os.close(child)
                    raise PermissionError("unsafe diagnostic directory")
                os.close(self.directory)
                self.directory = child
            os.fchmod(self.directory, 0o700)
            self.lockfile = self._open("writer.lock", os.O_RDWR | os.O_CREAT)
            fcntl.flock(self.lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
            # Previous configurations can have up to ten backups. Validate all
            # owned files before rotation, and prune only known surplus backups.
            for name in [self.name] + [self.name + f".{n}" for n in range(1, 11)]:
                try:
                    fd = self._open(name, os.O_RDONLY)
                except FileNotFoundError:
                    continue
                try:
                    size = os.fstat(fd).st_size
                    if name == self.name and size and os.pread(fd, 1, size - 1) != b"\n":
                        raise ValueError("incomplete diagnostic line")
                finally:
                    os.close(fd)
            for n in range(settings.backups + 1, 11):
                self._unlink(self.name + f".{n}")
            self.file = self._open(self.name, os.O_WRONLY | os.O_APPEND | os.O_CREAT)
        except BaseException:
            self.close()
            raise

    def _open(self, name, flags):
        fd = os.open(name, flags | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600, dir_fd=self.directory)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1:
                raise PermissionError("unsafe diagnostic file")
            os.fchmod(fd, 0o600)
            return fd
        except BaseException:
            os.close(fd)
            raise

    def _unlink(self, name):
        try:
            os.unlink(name, dir_fd=self.directory)
        except FileNotFoundError:
            pass

    def _rotate(self):
        os.close(self.file)
        self.file = None
        self._unlink(self.name + f".{self.settings.backups}")
        for n in range(self.settings.backups - 1, 0, -1):
            try:
                os.rename(self.name + f".{n}", self.name + f".{n+1}",
                          src_dir_fd=self.directory, dst_dir_fd=self.directory)
            except FileNotFoundError:
                pass
        os.rename(self.name, self.name + ".1", src_dir_fd=self.directory, dst_dir_fd=self.directory)
        self.file = self._open(self.name, os.O_WRONLY | os.O_APPEND | os.O_CREAT)

    def write(self, payload):
        if os.fstat(self.file).st_size + len(payload) > self.settings.max_bytes:
            self._rotate()
        view = memoryview(payload)
        while view:
            count = os.write(self.file, view)
            if count <= 0:
                raise OSError("diagnostic write failed")
            view = view[count:]

    def close(self):
        error = None
        for key in ("file", "lockfile", "directory"):
            fd = getattr(self, key, None)
            if fd is not None:
                try:
                    os.close(fd)
                except OSError as exc:
                    error = exc
                finally:
                    setattr(self, key, None)
        if error is not None:
            raise error


class DiagnosticSink:
    def __init__(self, settings=SinkSettings(), *, root=None):
        self.settings, self.root = settings, ROOT if root is None else root
        self.queue = Queue(maxsize=settings.capacity)
        self.lock, self.stop_event = Lock(), Event()
        self.counts = dict(submitted=0, written=0, dropped=0, invalid_records=0,
                           queue_full=0, sink_failures=0, initialization_failures=0,
                           shutdown_timeouts=0, inflight=0)
        self.accepting, self.state = True, "starting"
        self.thread = Thread(target=self._run, name="mooby-diagnostic-writer", daemon=True)

    def start(self):
        try:
            self.thread.start()
        except Exception:
            with self.lock:
                self.accepting, self.state = False, "failed"
                self.counts["initialization_failures"] += 1
                self.counts["sink_failures"] += 1
        return self

    def submit(self, record):
        try:
            payload = encode_record(record)
        except Exception:
            with self.lock:
                self.counts["submitted"] += 1
                self.counts["dropped"] += 1
                self.counts["invalid_records"] += 1
            return
        with self.lock:
            self.counts["submitted"] += 1
            if not self.accepting or not self.thread.is_alive():
                self.counts["dropped"] += 1
                return
            try:
                self.queue.put_nowait(payload)
            except Full:
                self.counts["queue_full"] += 1
                self.counts["dropped"] += 1

    def _run(self):
        writer = None
        initialized = False
        try:
            writer = _FileWriter(self.root, self.settings)
            initialized = True
            with self.lock:
                self.state = "draining" if self.stop_event.is_set() else "running"
            while not self.stop_event.is_set() or not self.queue.empty():
                try:
                    payload = self.queue.get(timeout=0.05)
                except Empty:
                    continue
                with self.lock:
                    self.counts["inflight"] = 1
                try:
                    writer.write(payload)
                except BaseException:
                    with self.lock:
                        self.counts["dropped"] += 1
                    raise
                else:
                    with self.lock:
                        self.counts["written"] += 1
                finally:
                    with self.lock:
                        self.counts["inflight"] = 0
                    self.queue.task_done()
            with self.lock:
                self.state = "stopped"
        except BaseException:
            # Never forward a writer traceback to stderr or another logger.
            with self.lock:
                self.state = "failed"
                self.counts["sink_failures"] += 1
                self.counts["initialization_failures"] += int(not initialized)
        finally:
            with self.lock:
                self.accepting = False
            if writer is not None:
                try:
                    writer.close()
                except BaseException:
                    with self.lock:
                        self.state = "failed"
                        self.counts["sink_failures"] += 1
            while True:
                try:
                    self.queue.get_nowait()
                except Empty:
                    break
                with self.lock:
                    self.counts["dropped"] += 1
                self.queue.task_done()

    def close(self):
        with self.lock:
            self.accepting = False
            if self.state in {"running", "starting"}:
                self.state = "draining"
        self.stop_event.set()
        if self.thread.ident is not None:
            self.thread.join(timeout=self.settings.flush_seconds)
        if self.thread.is_alive():
            with self.lock:
                self.counts["shutdown_timeouts"] += 1

    def snapshot(self):
        with self.lock:
            return dict(self.counts, pending=self.queue.qsize(), accepting=self.accepting,
                        writer_state=self.state, writer_alive=self.thread.is_alive())


_registry_lock = Lock()
_sink = None
_users = 0
_configuration_failures = 0


def submit_record(record):
    """True means configured/handled, including drops; never fall back to raw logs."""
    sink = _sink
    if sink is None or _users == 0:
        return False
    sink.submit(record)
    return True


def diagnostic_status():
    sink = _sink
    return dict(sink.snapshot() if sink is not None else {"writer_state": "disabled"},
                enabled=_users > 0, configuration_failures=_configuration_failures)


@asynccontextmanager
async def diagnostic_lifespan():
    global _sink, _users, _configuration_failures
    acquired = False
    with _registry_lock:
        enabled = os.environ.get("MOOBY_DIAGNOSTIC_ENABLED", "0")
        if enabled not in {"0", "1"}:
            _configuration_failures += 1
        if enabled == "1":
            try:
                settings = SinkSettings.from_environment()
                if _sink is None or (_users == 0 and not _sink.thread.is_alive()):
                    _sink = DiagnosticSink(settings).start()
                _users += 1
                acquired = True
            except Exception:
                _configuration_failures += 1
    try:
        yield
    finally:
        if acquired:
            with _registry_lock:
                _users -= 1
                if _users == 0:
                    _sink.close()
