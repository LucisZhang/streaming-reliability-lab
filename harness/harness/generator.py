from __future__ import annotations

import argparse
import random
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from harness.config import load_settings
from harness.sql import run_mysql_script

STATUSES = ("created", "paid", "packed", "shipped", "delivered")
BASE_TS = datetime(2026, 1, 1, tzinfo=UTC)


@dataclass(frozen=True)
class OrderEvent:
    order_id: int
    business_key: str
    event_id: int
    customer_id: int
    status: str
    amount_cents: int
    updated_at: datetime
    seed: int


def generate_events(events: int, seed: int) -> Iterable[OrderEvent]:
    if events < 1:
        raise ValueError("--events must be positive")
    rng = random.Random(seed)
    customer_span = max(1, min(events, 10_000))
    for event_id in range(1, events + 1):
        order_id = event_id
        yield OrderEvent(
            order_id=order_id,
            business_key=f"order-{order_id:012d}",
            event_id=event_id,
            customer_id=1 + rng.randrange(customer_span),
            status=STATUSES[rng.randrange(len(STATUSES))],
            amount_cents=100 + rng.randrange(250_000),
            updated_at=BASE_TS + timedelta(seconds=event_id),
            seed=seed,
        )


def _sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def _event_values(event: OrderEvent) -> str:
    timestamp = event.updated_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    return (
        f"({event.order_id},"
        f"{_sql_string(event.business_key)},"
        f"{event.event_id},"
        f"{event.customer_id},"
        f"{_sql_string(event.status)},"
        f"{event.amount_cents},"
        f"{_sql_string(timestamp)},"
        f"{event.seed})"
    )


def _chunks(items: Iterable[OrderEvent], size: int) -> Iterable[list[OrderEvent]]:
    chunk: list[OrderEvent] = []
    for item in items:
        chunk.append(item)
        if len(chunk) == size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def insert_events(events: int, seed: int, batch_size: int, reset: bool) -> None:
    settings = load_settings()
    if reset:
        reset_proc = run_mysql_script("TRUNCATE TABLE orders;", settings=settings, capture=True)
        if reset_proc.returncode != 0:
            raise RuntimeError(reset_proc.stderr.strip())

    for batch in _chunks(generate_events(events, seed), batch_size):
        values = ",\n".join(_event_values(event) for event in batch)
        columns = (
            "(order_id, business_key, event_id, customer_id, status, "
            "amount_cents, updated_at, seed)"
        )
        sql = "INSERT INTO orders " f"{columns} " f"VALUES\n{values};"
        proc = run_mysql_script(sql, settings=settings, capture=True)
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.strip())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Deterministically generate source rows into MySQL orders."
    )
    parser.add_argument("--events", type=int, required=True)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Truncate orders before inserting. Use only for explicit clean regeneration.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        insert_events(args.events, args.seed, args.batch_size, args.reset)
    except Exception as exc:
        print(f"generator failed: {exc}", file=sys.stderr)
        return 1
    print(
        "generator ok "
        f"events={args.events} seed={args.seed} "
        f"event_id_min=1 event_id_max={args.events}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
