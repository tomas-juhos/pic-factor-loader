"""Helper functions to deal with timeframes."""
from datetime import datetime, timedelta
from typing import List
import logging

logger = logging.getLogger(__name__)


def generate_quarters(years: List[int]):
    intervals = []
    for year in years:
        intervals = intervals + [
            (datetime(year, 1, 1), datetime(year, 6, 30)),
            (datetime(year, 7, 1), datetime(year, 12, 31)),
        ]

    return intervals


def generate_intervals(years: List[int]):
    intervals = []
    for year in years:
        first_day = datetime(year, 1, 1)
        if first_day.weekday() == 0:
            # first day monday, goes back to get last friday of the year
            intervals = intervals + [
                (first_day - timedelta(days=3), datetime(year, 12, 31))
            ]
        elif first_day.weekday() == 6:
            # first day sunday, goes back to get last friday of the year
            intervals = intervals + [
                (first_day - timedelta(days=2), datetime(year, 12, 31))
            ]
        else:
            # any other weekday has a weekday before
            intervals = intervals + [
                (first_day - timedelta(days=1), datetime(year, 12, 31))
            ]

    return intervals
