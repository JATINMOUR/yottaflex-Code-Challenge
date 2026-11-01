import csv
import hashlib
import io
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation


class RecordError(ValueError):
    def __init__(self, reason, detail):
        super().__init__(detail)
        self.reason = reason
        self.detail = detail


def parse_ts(value):
    value = value.strip()

    if value.endswith("Z"):
        value = value[:-1] + "+00:00"

    parsed = datetime.fromisoformat(value)

    if parsed.tzinfo is None:
        raise ValueError("timestamp must include timezone or Z")

    return parsed.astimezone(timezone.utc)


def parse_amount(value):
    amount = Decimal(value.strip())

    if not amount.is_finite() or amount < 0:
        raise ValueError("amount must be a non-negative number")

    cents = amount * 100

    if cents != cents.to_integral_value():
        raise ValueError("amount cannot have more than 2 decimal places")

    return int(cents)


def compute_record_hash(fields):
    payload = "\x1f".join(fields).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def parse_event(fields, column_indexes):
    values = {
        name: fields[index].strip()
        for name, index in column_indexes.items()
    }

    required = ("event_id", "source_system", "event_ts", "updated_at",
                "dataset_name", "code", "amount")

    for name in required:
        if not values[name]:
            raise RecordError("MISSING_REQUIRED_FIELD", name)

    try:
        event_ts = parse_ts(values["event_ts"])
        updated_at = parse_ts(values["updated_at"])
    except ValueError as exc:
        raise RecordError("INVALID_TIMESTAMP", str(exc)) from exc

    try:
        amount_cents = parse_amount(values["amount"])
    except (ValueError, InvalidOperation) as exc:
        raise RecordError("INVALID_AMOUNT", str(exc)) from exc

    values["dataset_name"] = values["dataset_name"].lower()
    values["code"] = values["code"].upper()
    values["event_ts"] = event_ts
    values["updated_at"] = updated_at
    values["amount_cents"] = amount_cents

    return values


def raw_record(fields):
    buffer = io.StringIO()
    csv.writer(buffer).writerow(fields)
    return buffer.getvalue().strip()