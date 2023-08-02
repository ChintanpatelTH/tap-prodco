"""Stream type classes for tap-prodco."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from dateutil import parser

from tap_prodco.client import ProdcoSourceStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class TrafficData(ProdcoSourceStream):
    """Define custom stream."""

    name = "traffic_data"
    path = "/TrafficData/Hourly"
    primary_keys = ["DateTime", "StoreNo"]  # noqa: RUF012
    replication_method = "INCREMENTAL"
    replication_key = "DateTime"
    schema_filepath = SCHEMAS_DIR / "traffics.json"
    records_jsonpath = "$.Data[*]"
    is_sorted = True
    check_sorted = False  # Skip checking sorting data

    def get_url_params(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: Any | None,  # noqa: ANN401, ARG002
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        params["from"] = self.start_date
        params["to"] = self.end_date
        params["increment"] = "FIFTEEN_MINUTES"
        self.logger.info(params)
        return params

    def get_records(self, context: dict) -> Iterable[dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        Args:
            context: The stream context.

        Yields:
            Each record from the source.
        """
        current_state = self.get_context_state(context)
        current_date = datetime.now(timezone.utc).replace(tzinfo=None)
        interval = float(self.config.get("backfill_interval", 1))
        min_value = current_state.get(
            "replication_key_value",
            self.config.get("start_date", ""),
        )
        context = context or {}
        # set from date to last updated date or config start date
        min_date = parser.parse(min_value) + timedelta(seconds=1)
        while min_date < current_date:
            updated_at_max = min_date + timedelta(days=interval)
            if updated_at_max > current_date:
                updated_at_max = current_date

            self.start_date = min_date.isoformat()
            self.end_date = updated_at_max.isoformat()
            yield from super().get_records(context)
            # Send state message
            self._increment_stream_state({"DateTime": self.end_date}, context=context)
            self._write_state_message()
            min_date = updated_at_max


class StoresData(ProdcoSourceStream):
    """Define custom stream."""

    name = "stores_data"
    path = "/Stores"
    primary_keys = ["StoreNo"]  # noqa: RUF012
    replication_method = "FULL_TABLE"
    schema_filepath = SCHEMAS_DIR / "stores.json"
    records_jsonpath = "$.Data[*]"
