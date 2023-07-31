"""Stream type classes for tap-prodco."""

from __future__ import annotations

from pathlib import Path

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_prodco.client import ProdcoSourceStream

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class TrafficData(ProdcoSourceStream):
    """Define custom stream."""

    name = "traffic_data"
    path = "/TrafficData/Hourly"
    primary_keys = ["DateTime","StoreNo"]  # noqa: RUF012
    replication_method = "INCREMENTAL"
    replication_key = "DateTime"
    schema_filepath = SCHEMAS_DIR / "stores.json"
    records_jsonpath = "$.Data[*]"
    is_sorted = False

class StoresData(ProdcoSourceStream):
    """Define custom stream."""

    name = "stores_data"
    path = "/Stores"
    primary_keys = ["StoreNo"]  # noqa: RUF012
    replication_method = "FULL_TABLE"
    schema_filepath = SCHEMAS_DIR / "stores.json"
    records_jsonpath = "$.Data[*]"