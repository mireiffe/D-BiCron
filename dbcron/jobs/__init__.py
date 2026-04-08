"""Job registry — maps job names to their implementing classes."""

JOB_REGISTRY: dict[str, type] = {}

__all__ = ["JOB_REGISTRY"]
