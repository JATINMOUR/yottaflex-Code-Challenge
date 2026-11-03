"""Row-level parsing and field validation for incoming events (pure functions, no I/O)."""
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation

from . import rules
from .util import short_hash


class RecordError(Exception):
    def __init__(self, reason, detail):
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail


@dataclass(frozen=True)
class Event:
    line_no: int
    event_id: str
    source_system: str
    event_ts: str          # normalised UTC, ISO-8601 'YYYY-MM-DDTHH:MM:SSZ'
    updated_at: str        # normalised UTC
    effective_date: date   # UTC calendar date of event_ts: the date used for reference lookup
    dataset_name: str      # lower-cased
    code: str              # upper-cased
    amount_cents: int
    record_hash: str


def parse_ts(value):
    """Strict ISO-8601 with an explicit offset ('Z' or +hh:mm). Returns aware UTC datetime."""
    v = value.strip()
    if v.endswith(("Z", "z")):
        v = v[:-1] + "+00:00"
    dt = datetime.fromisoformat(v)  # ValueError on garbage
    if dt.tzinfo is None:
        raise ValueError("timezone offset required")
    return dt.astimezone(timezone.utc)


def fmt_ts(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_amount(value):
    """Non-negative decimal with at most 2 fraction digits -> integer cents (exact)."""
    try:
        d = Decimal(value.strip())
    except InvalidOperation:
        raise ValueError(f"not a number: {value!r}")
    if not d.is_finite():
        raise ValueError(f"not finite: {value!r}")
    if d < 0:
        raise ValueError(f"negative amount: {value!r}")
    if d.as_tuple().exponent < -2:
        raise ValueError(f"more than 2 decimal places: {value!r}")
    return int((d * 100).to_integral_value())


def compute_record_hash(fields):
    """Hash of the stripped raw payload; same payload => same hash (drives idempotency)."""
    return short_hash(*[f.strip() for f in fields])


def best_effort_amount(fields, idx):
    try:
        return parse_amount(fields[idx["amount"]])
    except (ValueError, IndexError):
        return None


def parse_event(line_no, fields, ncols, idx):
    """Validate one CSV row. Raises RecordError (first failing rule wins).

    Rule order: column count -> required fields -> timestamps -> amount.
    """
    if len(fields) != ncols:
        raise RecordError(rules.MALFORMED_RECORD, f"expected {ncols} fields, found {len(fields)}")
    val = {c: fields[i].strip() for c, i in idx.items()}
    empty = [c for c in rules.REQUIRED_EVENT_COLUMNS if not val[c]]
    if empty:
        raise RecordError(rules.MISSING_REQUIRED_FIELD, "empty: " + ",".join(empty))
    try:
        ts = parse_ts(val["event_ts"])
    except ValueError as exc:
        raise RecordError(rules.INVALID_TIMESTAMP, f"event_ts {val['event_ts']!r}: {exc}")
    try:
        upd = parse_ts(val["updated_at"])
    except ValueError as exc:
        raise RecordError(rules.INVALID_TIMESTAMP, f"updated_at {val['updated_at']!r}: {exc}")
    try:
        cents = parse_amount(val["amount"])
    except ValueError as exc:
        raise RecordError(rules.INVALID_AMOUNT, str(exc))
    payload = [fields[idx[c]] for c in rules.REQUIRED_EVENT_COLUMNS]
    return Event(
        line_no=line_no, event_id=val["event_id"], source_system=val["source_system"],
        event_ts=fmt_ts(ts), updated_at=fmt_ts(upd), effective_date=ts.date(),
        dataset_name=val["dataset_name"].lower(), code=val["code"].upper(),
        amount_cents=cents, record_hash=compute_record_hash(payload),
    )


def raw_json(fields):
    return json.dumps(fields, ensure_ascii=False)
