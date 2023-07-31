"""ProdcoSource tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_prodco import streams


class TapProdcoSource(Tap):
    """ProdcoSource tap class."""

    name = "tap-prodco"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "username",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="Prodco Username",
        ),
        th.Property(
            "password",
            th.StringType,
            required=True,
            description="Prodco Password",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=True,
            description="The earliest record date to sync",
        ),
        th.Property(
            "backfill_interval",
            th.IntegerType,
            required=True,
            description="Backfill interval in days",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.ProdcoSourceStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.TrafficData(self),
            streams.StoresData(self),
        ]


if __name__ == "__main__":
    TapProdcoSource.cli()
