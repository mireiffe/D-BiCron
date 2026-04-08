"""SQLAlchemy engine factory for susdb and coredb."""

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from .config import AppConfig


def create_susdb_engine(config: AppConfig) -> Engine:
    return create_engine(config.susdb.url, pool_pre_ping=True)


def create_coredb_engine(config: AppConfig) -> Engine:
    return create_engine(config.coredb.url, pool_pre_ping=True)
