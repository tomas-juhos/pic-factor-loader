"""Factor Loader Config queries."""


class Queries:
    """Factor Loader Config queries class."""

    UPSERT = (
        "INSERT INTO factor_loader_config ("
        "       factor, "
        "       timeframe, "
        "       last_date_persisted, "
        "       source_table "
        ") VALUES %s "
        "ON CONFLICT (factor, timeframe) DO "
        "UPDATE SET "
        "       last_date_persisted=EXCLUDED.last_date_persisted; "
    )
