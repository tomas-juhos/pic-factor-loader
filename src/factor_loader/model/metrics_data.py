"""Metrics model."""

from datetime import datetime
from decimal import Decimal
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class MetricsData:
    """Metrics record object class."""

    datadate: datetime
    gvkey: int

    utilization_pct_delta: Optional[Decimal] = None
    bar_delta: Optional[int] = None
    age_delta: Optional[Decimal] = None
    tickets_delta: Optional[int] = None
    units_delta: Optional[Decimal] = None
    market_value_usd_delta: Optional[Decimal] = None
    loan_rate_avg_delta: Optional[Decimal] = None
    loan_rate_max_delta: Optional[Decimal] = None
    loan_rate_min_delta: Optional[Decimal] = None
    loan_rate_range_delta: Optional[Decimal] = None
    loan_rate_stdev_delta: Optional[Decimal] = None

    short_interest: Optional[Decimal] = None
    short_ratio: Optional[Decimal] = None

    market_cap: Optional[Decimal] = None
    shares_out: Optional[Decimal] = None
    volume: Optional[Decimal] = None
    rtn: Optional[Decimal] = None
    winsorized_5_rtn: Optional[Decimal] = None

    @classmethod
    def build_record(cls, record) -> "MetricsData":
        res = cls()

        res.datadate = record[0]
        res.gvkey = record[1]
        res.utilization_pct_delta = record[2]
        res.bar_delta = record[3]
        res.age_delta = record[4]
        res.tickets_delta = record[5]
        res.units_delta = record[6]
        res.market_value_usd_delta = record[7]
        res.loan_rate_avg_delta = record[8]
        res.loan_rate_max_delta = record[9]
        res.loan_rate_min_delta = record[10]
        res.loan_rate_range_delta = record[11]
        res.loan_rate_stdev_delta = record[12]
        res.short_interest = record[13]
        res.short_ratio = record[14]
        res.market_cap = record[15]
        res.shares_out = record[16]
        res.volume = record[17]
        res.rtn = record[18]
        res.winsorized_5_rtn = record[19]

        return res
