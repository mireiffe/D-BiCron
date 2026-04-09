"""Job registry — auto-discovers Job subclasses from this package."""

import importlib
import logging
import pkgutil

from .base import Job

logger = logging.getLogger(__name__)

JOB_REGISTRY: dict[str, type] = {}

for _finder, modname, _ispkg in pkgutil.iter_modules(__path__):
    if modname == "base":
        continue
    try:
        module = importlib.import_module(f".{modname}", __package__)
    except Exception:
        logger.warning("Failed to import job module: %s", modname, exc_info=True)
        continue
    for attr in dir(module):
        obj = getattr(module, attr)
        if (
            isinstance(obj, type)
            and issubclass(obj, Job)
            and obj is not Job
            and getattr(obj, "name", "base") != "base"
        ):
            JOB_REGISTRY[obj.name] = obj

__all__ = ["JOB_REGISTRY"]
