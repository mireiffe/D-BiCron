"""Job registry — maps job names to their implementing classes."""

from .recommendation import RecommendationJob

JOB_REGISTRY: dict[str, type] = {
    "recommendation": RecommendationJob,
}

__all__ = ["JOB_REGISTRY"]
