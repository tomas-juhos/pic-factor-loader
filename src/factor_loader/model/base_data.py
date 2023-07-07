"""Base Data model."""

from datetime import datetime
from decimal import Decimal
import logging
from typing import Optional


logger = logging.getLogger(__name__)


class BaseData:
    """Aggregate base record object class."""

    datadate: datetime
    gvkey: int

    utilization_pct: Optional[Decimal] = None
    bar: Optional[Decimal] = None
    age: Optional[Decimal] = None
    tickets: Optional[Decimal] = None
    units: Optional[Decimal] = None
    market_value_usd: Optional[Decimal] = None
    loan_rate_avg: Optional[Decimal] = None
    loan_rate_max: Optional[Decimal] = None
    loan_rate_min: Optional[Decimal] = None
    loan_rate_range: Optional[Decimal] = None
    loan_rate_stdev: Optional[Decimal] = None

    market_cap: Optional[Decimal] = None
    shares_out: Optional[Decimal] = None
    volume: Optional[Decimal] = None
    rtn: Optional[Decimal] = None
    winsorized_5_rtn: Optional[Decimal] = None

    @classmethod
    def build_record(cls, record):
        res = cls()

        res.datadate = record[0]
        res.gvkey = record[1]
        res.utilization_pct = record[2]
        res.bar = record[3]
        res.age = record[4]
        res.tickets = record[5]
        res.units = record[6]
        res.market_value_usd = record[7]
        res.loan_rate_avg = record[8]
        res.loan_rate_max = record[9]
        res.loan_rate_min = record[10]
        res.loan_rate_range = record[11]
        res.loan_rate_stdev = record[12]
        res.market_cap = record[13]
        res.shares_out = record[14]
        res.volume = record[15]
        res.rtn = record[16]
        res.winsorized_5_rtn = record[17]

        return res
