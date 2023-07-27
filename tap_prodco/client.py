"""REST client handling, including ProdcoSourceStream base class."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

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

    @cached_property
    def authenticator(self) -> _Auth:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return ProdcoSourceAuthenticator.create_for_stream(self)

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,  # noqa: ARG002
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        if self.replication_key:
            current_date = datetime.now(timezone.utc).replace(tzinfo=None)
            context_state = self.get_context_state(context)
            last_updated = context_state.get("replication_key_value")
            interval = self.config.get("backfill_interval")
            config_start_date = self.config.get("start_date")
            # set from date to last updated date or config start date
            start_date = datetime.strptime(  # noqa: DTZ007
                (last_updated if last_updated else config_start_date),
                "%Y-%m-%dT%H:%M:%S",
            )
            params["from"] = start_date.strftime("%Y-%m-%d")
            # set to date to current date or start date + interval
            params["to"] = (
                current_date.strftime("%Y-%m-%d")
                if start_date + timedelta(days=interval) > current_date
                else (start_date + timedelta(days=interval)).strftime(
                    "%Y-%m-%d",
                )
            )
            params["increment"] = "FIFTEEN_MINUTES"
        return params
