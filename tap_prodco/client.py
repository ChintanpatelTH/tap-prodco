"""REST client handling, including ProdcoSourceStream base class."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Callable, Iterable

import requests
from singer_sdk.streams import RESTStream

from tap_prodco.auth import ProdcoSourceAuthenticator

if sys.version_info >= (3, 8):
    from functools import cached_property
else:
    from cached_property import cached_property

_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class ProdcoSourceStream(RESTStream):
    """ProdcoSource stream class."""

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return "https://api.prodcotech.com/api"

    records_jsonpath = "$[*]"  # Or override `parse_response`.

    # Prdco API does not support pagination
    next_page_token_jsonpath = "$.dummy"  # noqa: S105
    start_date: str | None = None
    end_date: str | None = None

    @cached_property
    def authenticator(self) -> _Auth:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return ProdcoSourceAuthenticator.create_for_stream(self)
