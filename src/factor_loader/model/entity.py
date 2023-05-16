"""Entity file."""

from enum import Enum


class Entity(str, Enum):
    """Entities."""

    FACTOR_RETURNS = "FACTOR_RETURNS"

    def __repr__(self) -> str:
        return str(self.value)
