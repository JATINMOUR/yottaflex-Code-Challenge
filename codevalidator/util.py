"""Small shared helpers: time, hashing, CSV reading, logging."""
import csv
import hashlib
import logging
import os
import sys
import uuid
from datetime import datetime, timezone

LOG = logging.getLogger("codevalidator")


class PipelineError(Exception):
    """A run-level failure (bad file/schema/reference data). Carries a stable code."""

    def __init__(self, code, message=""):
        super().__init__(f"{code}: {message}" if message else code)
        self.code = code
        self.message = message


def utc_now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def new_run_id(prefix="run"):
    return f"{prefix}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def short_hash(*parts):
    return hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()[:16]


def read_csv(path):
    """Return (header, [(physical_line_no, fields)], blank_lines_skipped).

    Raises PipelineError for missing/zero-byte/unreadable files. Blank lines are skipped
    and counted, never silently lost. Header-only files return an empty row list.
    """
    if not os.path.isfile(path):
        raise PipelineError("INPUT_NOT_FOUND", path)
    if os.path.getsize(path) == 0:
        raise PipelineError("EMPTY_FILE", "zero-byte file (no header)")
    rows, blanks = [], 0
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if header is None:
                raise PipelineError("EMPTY_FILE", "no header row")
            for fields in reader:
                if not fields:
                    blanks += 1
                    continue
                rows.append((reader.line_num, fields))
    except (UnicodeDecodeError, csv.Error) as exc:
        raise PipelineError("UNREADABLE_FILE", str(exc))
    return header, rows, blanks


def setup_logging(level=logging.INFO, log_file=None):
    handlers = [logging.StreamHandler(sys.stderr)]
    if log_file:
        os.makedirs(os.path.dirname(os.path.abspath(log_file)), exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    logging.basicConfig(
        level=level, handlers=handlers, force=True,
        format="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
