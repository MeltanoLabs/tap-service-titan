"""Tests for the custom-report async data-query polling in reporting.py."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from http import HTTPStatus
from typing import Any

import pytest
import requests
from singer_sdk.exceptions import RetriableAPIError

from tap_service_titan.streams.reporting import ReportPoller

PAYLOAD = {
    "fields": [{"name": "id", "label": "ID"}, {"name": "name", "label": "Name"}],
    "data": [[1, "Alice"], [2, "Bob"]],
}


@dataclass
class _FakeResponse:
    """A minimal stand-in for `requests.Response` - no `unittest.mock` required."""

    status_code: int
    payload: dict[str, Any] = field(default_factory=dict)

    def json(self, **kwargs: Any) -> dict[str, Any]:  # noqa: ARG002
        return self.payload


def _fail_if_called(_seconds: float) -> None:
    pytest.fail("poller should not have slept")


class TestReportPoller:
    """Unit tests for `ReportPoller` in isolation - no stream, tap, or HTTP mocking."""

    def test_returns_immediately_on_first_non_202(self) -> None:
        ok = requests.Response()
        ok.status_code = HTTPStatus.OK
        ok._content = json.dumps(PAYLOAD).encode()  # noqa: SLF001

        poller = ReportPoller(
            fetch=lambda url: ok,  # noqa: ARG005
            validate=lambda response: None,  # noqa: ARG005
            sleep=_fail_if_called,
        )

        assert poller.poll("https://example.test/data-queries/token") == PAYLOAD

    def test_polls_until_ready(self) -> None:
        accepted = requests.Response()
        accepted.status_code = HTTPStatus.ACCEPTED
        accepted._content = b'{"token": "t"}'  # noqa: SLF001

        ok = requests.Response()
        ok.status_code = HTTPStatus.OK
        ok._content = json.dumps(PAYLOAD).encode()  # noqa: SLF001

        responses = iter([
            accepted,
            accepted,
            ok,
        ])
        sleeps: list[float] = []
        poller = ReportPoller(
            fetch=lambda url: next(responses),  # noqa: ARG005
            validate=lambda response: None,  # noqa: ARG005
            sleep=sleeps.append,
            poll_interval_seconds=2.0,
        )

        assert poller.poll("https://example.test/data-queries/token") == PAYLOAD
        assert sleeps == [2.0, 2.0]

    def test_raises_after_max_wait(self) -> None:
        accepted = requests.Response()
        accepted.status_code = HTTPStatus.ACCEPTED
        accepted._content = b'{"token": "t"}'  # noqa: SLF001

        poller = ReportPoller(
            fetch=lambda url: accepted,  # noqa: ARG005
            validate=lambda response: None,  # noqa: ARG005
            sleep=lambda _seconds: None,
            poll_interval_seconds=1.0,
            max_wait_seconds=2.0,
        )

        with pytest.raises(RetriableAPIError):
            poller.poll("https://example.test/data-queries/token")

    def test_propagates_validate_errors(self) -> None:
        def _validate(response: requests.Response) -> None:
            if response.status_code >= HTTPStatus.BAD_REQUEST:
                msg = "boom"
                raise RetriableAPIError(msg)

        error = requests.Response()
        error.status_code = HTTPStatus.INTERNAL_SERVER_ERROR

        poller = ReportPoller(
            fetch=lambda url: error,  # noqa: ARG005
            validate=_validate,
            sleep=_fail_if_called,
        )

        with pytest.raises(RetriableAPIError, match="boom"):
            poller.poll("https://example.test/data-queries/token")

    def test_records_from_payload_reads_pascal_case_keys(self) -> None:
        """The live API returns PascalCase keys, unlike the OpenAPI spec's lowercase example."""
        payload = {
            "Fields": [{"Name": "id", "Label": "ID"}, {"Name": "name", "Label": "Name"}],
            "Data": [[1, "Alice"], [2, None]],
        }

        records = list(ReportPoller.records_from_payload(payload))

        assert records == [
            {"id": "1", "name": "Alice"},
            {"id": "2", "name": ""},
        ]
