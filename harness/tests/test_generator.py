from __future__ import annotations

from harness.generator import generate_events


def test_same_seed_generates_same_stream() -> None:
    first = list(generate_events(25, 1))
    second = list(generate_events(25, 1))
    assert first == second


def test_event_ids_are_monotonic_and_unique() -> None:
    events = list(generate_events(100, 7))
    event_ids = [event.event_id for event in events]
    assert event_ids == list(range(1, 101))
    assert len(set(event.business_key for event in events)) == 100
