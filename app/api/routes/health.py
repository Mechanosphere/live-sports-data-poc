"""Liveness/readiness probe for the API process.

Deliberately has no dependencies on DynamoDB/Kafka/Postgres -- it only
confirms the FastAPI process itself is up and serving requests, so it stays
fast and reliable even if a downstream infra dependency is degraded.
"""
from fastapi import APIRouter

router = APIRouter()


@router.get("/health")
def health_check():
    return {"status": "ok"}
