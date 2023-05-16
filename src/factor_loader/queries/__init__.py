"""Queries implementation."""

from .factor_loader_config import Queries as ConfigQueries
from .factor_returns import Queries as FactorReturnsQueries


__all__ = [
    "ConfigQueries",
    "FactorReturnsQueries",
]
