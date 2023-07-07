"""Config model."""

from datetime import datetime
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class Config:
    """Metrics record object class."""

    factor: str
    timeframe: str
    mkt_cap_class: str
    top: int
    last_date_persisted: Optional[datetime] = None
    source_table: str

    @classmethod
    def build_record(cls, record) -> "Config":
        res = cls()

        res.factor = str(record[0]).lower()
        res.timeframe = str(record[1]).lower()
        res.last_date_persisted = record[2]
        res.source_table = str(record[3]).lower()

        return res

    def __repr__(self):
        return f"({self.factor.upper()}, {self.timeframe.upper()})"
