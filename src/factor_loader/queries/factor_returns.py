"""Factor Returns queries."""


class Queries:
    """Factor Returns queries class."""

    UPSERT = (
        "INSERT INTO factor_returns ("
        "       datadate, "
        "       factor, "
        "       timeframe, "
        "       mkt_cap_class, "
        "       top, "
        "       long_rtn, "
        "       short_rtn, "
        "       rtn, "
        "       consistent "
        ") VALUES %s "
        "ON CONFLICT (datadate, factor, timeframe, mkt_cap_class, top) DO "
        "UPDATE SET "
        "       datadate=EXCLUDED.datadate, "
        "       factor=EXCLUDED.factor, "
        "       timeframe=EXCLUDED.timeframe, "
        "       mkt_cap_class=EXCLUDED.mkt_cap_class, "
        "       top=EXCLUDED.top, "
        "       long_rtn=EXCLUDED.long_rtn, "
        "       short_rtn=EXCLUDED.short_rtn, "
        "       rtn=EXCLUDED.rtn, "
        "       consistent=EXCLUDED.consistent; "
    )
