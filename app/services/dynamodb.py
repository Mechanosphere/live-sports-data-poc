import logging
import time
from functools import lru_cache

import boto3
from boto3.resources.base import ServiceResource

from app.core.config import get_settings

logger = logging.getLogger(__name__)

# Single source of truth for the table's GSIs, and the attributes each one
# needs. Used both when creating the table from scratch and when
# back-filling a missing index onto a table that already exists (see
# `_ensure_indexes` below) -- adding a new GSI here (e.g. a future GSI3)
# only requires listing it once.
_ATTRIBUTE_DEFINITIONS = {
    "PK": "S",
    "SK": "S",
    "GSI1PK": "S",
    "GSI1SK": "S",
    "GSI2PK": "S",
    "GSI2SK": "S",
}

_GLOBAL_SECONDARY_INDEXES = [
    {
        "IndexName": "GSI1",
        "KeySchema": [
            {"AttributeName": "GSI1PK", "KeyType": "HASH"},
            {"AttributeName": "GSI1SK", "KeyType": "RANGE"},
        ],
        "Projection": {"ProjectionType": "ALL"},
    },
    {
        "IndexName": "GSI2",
        "KeySchema": [
            {"AttributeName": "GSI2PK", "KeyType": "HASH"},
            {"AttributeName": "GSI2SK", "KeyType": "RANGE"},
        ],
        "Projection": {"ProjectionType": "ALL"},
    },
]


@lru_cache
def get_dynamodb_resource() -> ServiceResource:
    """Return a cached boto3 DynamoDB resource.

    Uses `dynamodb_endpoint_url` from settings so it can point at
    DynamoDB Local (e.g. http://localhost:8000) in development, or be
    left unset to use the real AWS DynamoDB service.
    """
    settings = get_settings()
    logger.info(
        "Creating DynamoDB resource (endpoint_url=%s region=%s)",
        settings.dynamodb_endpoint_url,
        settings.aws_region,
    )
    return boto3.resource(
        "dynamodb",
        endpoint_url=settings.dynamodb_endpoint_url,
        region_name=settings.aws_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )


@lru_cache
def get_dynamodb_client():
    """Return a cached boto3 DynamoDB low-level client."""
    settings = get_settings()
    return boto3.client(
        "dynamodb",
        endpoint_url=settings.dynamodb_endpoint_url,
        region_name=settings.aws_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )


@lru_cache
def get_table():
    """Return the (cached) boto3 Table resource for the app's single table.

    `Table()` itself makes no API call, but constructing it is needless
    per-call overhead in every repository function; the table name never
    changes at runtime so it's safe to cache for the life of the process.
    """
    settings = get_settings()
    return get_dynamodb_resource().Table(settings.dynamodb_table_name)


def _wait_for_gsi_active(client, table_name: str, index_name: str, timeout: float = 60.0) -> None:
    """Poll `describe_table` until `index_name` reports status ACTIVE.

    boto3 has no built-in waiter for GSI backfill completion (only for
    table-level status), so this rolls a small manual poll loop.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        desc = client.describe_table(TableName=table_name)["Table"]
        indexes = {gsi["IndexName"]: gsi["IndexStatus"] for gsi in desc.get("GlobalSecondaryIndexes", [])}
        if indexes.get(index_name) == "ACTIVE":
            return
        time.sleep(1.0)
    raise TimeoutError(f"Timed out waiting for index {index_name} on {table_name} to become ACTIVE")


def _ensure_indexes(client, table_name: str) -> None:
    """Back-fill any GSIs missing from an already-existing table.

    Lets newly added GSIs (e.g. GSI2, added for `get_top_events`) apply to
    existing local/dev tables via `UpdateTable` instead of requiring the
    table to be dropped and recreated. DynamoDB only allows one GSI
    create/delete per `UpdateTable` call, so missing indexes are added one
    at a time, waiting for each to finish backfilling before starting the
    next.
    """
    desc = client.describe_table(TableName=table_name)["Table"]
    existing_index_names = {gsi["IndexName"] for gsi in desc.get("GlobalSecondaryIndexes", [])}
    missing = [gsi for gsi in _GLOBAL_SECONDARY_INDEXES if gsi["IndexName"] not in existing_index_names]
    if not missing:
        return

    existing_attr_names = {attr["AttributeName"] for attr in desc.get("AttributeDefinitions", [])}
    for gsi in missing:
        needed_attrs = {key["AttributeName"] for key in gsi["KeySchema"]} - existing_attr_names
        logger.info("Table %s missing index %s, adding it now...", table_name, gsi["IndexName"])
        client.update_table(
            TableName=table_name,
            AttributeDefinitions=[
                {"AttributeName": name, "AttributeType": _ATTRIBUTE_DEFINITIONS[name]}
                for name in needed_attrs
            ],
            GlobalSecondaryIndexUpdates=[{"Create": gsi}],
        )
        _wait_for_gsi_active(client, table_name, gsi["IndexName"])
        existing_attr_names |= needed_attrs
        logger.info("Index %s on table %s is now ACTIVE.", gsi["IndexName"], table_name)


def create_table_if_not_exists() -> None:
    """Create the single table (with GSI1/GSI2) if it doesn't already exist.

    If the table already exists but is missing a GSI that's been added to
    `_GLOBAL_SECONDARY_INDEXES` since it was first created (e.g. an older
    local DynamoDB Local table predating GSI2), back-fill the missing
    index in place via `UpdateTable` rather than requiring a manual table
    drop/recreate.

    Intended for local/dev use against DynamoDB Local. In real AWS
    environments the table should be provisioned via infrastructure-as-code.
    """
    settings = get_settings()
    client = get_dynamodb_client()

    existing_tables = client.list_tables().get("TableNames", [])
    if settings.dynamodb_table_name in existing_tables:
        logger.info("Table %s already exists, checking indexes...", settings.dynamodb_table_name)
        _ensure_indexes(client, settings.dynamodb_table_name)
        return

    logger.info("Creating table %s (with GSI1/GSI2)...", settings.dynamodb_table_name)
    client.create_table(
        TableName=settings.dynamodb_table_name,
        AttributeDefinitions=[
            {"AttributeName": name, "AttributeType": attr_type}
            for name, attr_type in _ATTRIBUTE_DEFINITIONS.items()
        ],
        KeySchema=[
            {"AttributeName": "PK", "KeyType": "HASH"},
            {"AttributeName": "SK", "KeyType": "RANGE"},
        ],
        GlobalSecondaryIndexes=_GLOBAL_SECONDARY_INDEXES,
        BillingMode="PAY_PER_REQUEST",
    )
    client.get_waiter("table_exists").wait(TableName=settings.dynamodb_table_name)
    logger.info("Table %s created.", settings.dynamodb_table_name)
