"""Test pagination."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import requests
import time_machine

from tap_service_titan.streams.dispatch import CapacitiesPaginator, CapacitiesStream
from tap_service_titan.tap import TapServiceTitan

_DUMMY_CONFIG = {
    "client_id": "x",
    "client_secret": "x",
    "st_app_key": "x",
    "tenant_id": "x",
}

_DEFAULT_LOOKAHEAD_DAYS = 14
_CUSTOM_LOOKAHEAD_DAYS = 90


def test_capacities_paginator() -> None:
    """Test capacities paginator."""
    fake_now = datetime(2025, 1, 25, tzinfo=timezone.utc)
    start_date = fake_now - timedelta(days=30)
    response = Mock(spec=requests.Response)

    with time_machine.travel(fake_now):
        paginator = CapacitiesPaginator(start_value=start_date)

        # Should start at 30 days ago
        assert paginator.current_value == start_date
        assert (paginator.end_value - fake_now).days == paginator.lookahead_days
        assert paginator.has_more(response=response)

        # Advance through all remaining days to reach the end_value
        for _ in range(paginator.lookahead_days + 30):
            paginator.advance(response=response)

        # After reaching end_value, should still have_more (because <= check)
        assert paginator.current_value == paginator.end_value
        assert paginator.has_more(response=response)

        # One more advance to go past end_value
        paginator.advance(response=response)
        assert not paginator.has_more(response=response)

        # Final value should be 1 day after the end value
        assert (paginator.current_value - paginator.end_value).days == 1


def test_capacities_paginator_custom_lookahead() -> None:
    """The paginator window end honors a custom lookahead_days."""
    fake_now = datetime(2025, 1, 25, tzinfo=timezone.utc)

    with time_machine.travel(fake_now):
        paginator = CapacitiesPaginator(start_value=fake_now, lookahead_days=_CUSTOM_LOOKAHEAD_DAYS)

        assert paginator.lookahead_days == _CUSTOM_LOOKAHEAD_DAYS
        assert (paginator.end_value - fake_now).days == _CUSTOM_LOOKAHEAD_DAYS


def test_capacities_stream_lookahead_from_config() -> None:
    """The stream reads capacities_lookahead_days and pins the window start to today."""
    fake_now = datetime(2025, 1, 25, 9, 30, tzinfo=timezone.utc)
    config = {**_DUMMY_CONFIG, "capacities_lookahead_days": _CUSTOM_LOOKAHEAD_DAYS}
    start_of_today = fake_now.replace(hour=0, minute=0, second=0, microsecond=0)

    with time_machine.travel(fake_now):
        tap = TapServiceTitan(config=config, parse_env_config=False, validate_config=False)
        paginator = CapacitiesStream(tap=tap).get_new_paginator()

        # Window starts at the beginning of the current day, ignoring start_date...
        assert paginator.current_value == start_of_today
        # ...and ends capacities_lookahead_days out.
        assert paginator.lookahead_days == _CUSTOM_LOOKAHEAD_DAYS
        assert (paginator.end_value - fake_now).days == _CUSTOM_LOOKAHEAD_DAYS


def test_capacities_stream_lookahead_defaults_to_14() -> None:
    """Without config, the stream falls back to the historical 14-day window."""
    fake_now = datetime(2025, 1, 25, tzinfo=timezone.utc)

    with time_machine.travel(fake_now):
        tap = TapServiceTitan(config=_DUMMY_CONFIG, parse_env_config=False, validate_config=False)
        paginator = CapacitiesStream(tap=tap).get_new_paginator()

        assert paginator.lookahead_days == _DEFAULT_LOOKAHEAD_DAYS
