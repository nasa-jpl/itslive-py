"""
Pytest configuration and shared fixtures.

Network-hitting tests are marked ``integration`` and are skipped in CI
unless explicitly requested with ``-m integration``.
"""

import pytest
import responses as responses_lib


@pytest.fixture
def mock_responses():
    """Activate the ``responses`` library to intercept all HTTP requests.

    Usage inside a test::

        def test_something(mock_responses):
            mock_responses.add(
                responses_lib.GET,
                "https://example.com/api",
                json={"key": "value"},
                status=200,
            )
            # ... call code that makes HTTP requests
    """
    with responses_lib.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        yield rsps
