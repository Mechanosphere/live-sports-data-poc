"""Create the single DynamoDB table (with GSI1) for local development.

Usage:
    python3 -m scripts.create_table
"""
from app.services.dynamodb import create_table_if_not_exists
from app.core.config import get_settings


def main() -> None:
    settings = get_settings()
    create_table_if_not_exists()
    print(f"Table '{settings.dynamodb_table_name}' is ready at {settings.dynamodb_endpoint_url}")


if __name__ == "__main__":
    main()
