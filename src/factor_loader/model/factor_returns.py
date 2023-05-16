"""Returns model."""

from datetime import datetime
from decimal import Decimal
import logging
from typing import Optional, Tuple

from factor_loader.model.base import Modeling

logger = logging.getLogger(__name__)


class FactorReturns(Modeling):
    """Returns record object class."""

    datadate: datetime
    factor: str
    timeframe: str
    mkt_cap_class: str
    top: int

    long_rtn: Optional[Decimal] = None
    short_rtn: Optional[Decimal] = None
    rtn: Optional[Decimal] = None
    consistent: Optional[bool] = None

    @classmethod
    def build_record(cls, record: Tuple) -> "FactorReturns":
        """Builds Returns record object.

        Args:
            record: record from ciq market cap table.

        Returns:
            Returns record object.
        """
        res = cls()

        res.datadate = record[0]
        res.factor = record[1]
        res.timeframe = record[2]
        res.mkt_cap_class = record[3]
        res.top = record[4]
        res.long_rtn = record[5]
        res.short_rtn = record[6]
        res.rtn = record[7]
        res.consistent = record[8]

        return res

    def as_tuple(self) -> Tuple:
        """Get tuple with object attributes.

        Returns:
            Tuple with object attributes.
        """
        return (
            self.datadate,
            self.factor,
            self.timeframe,
            self.mkt_cap_class,
            self.top,
            self.long_rtn,
            self.short_rtn,
            self.rtn,
            self.consistent,
        )
