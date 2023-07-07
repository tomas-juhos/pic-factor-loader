import os

from factor_loader.persistence.target import Target
import factor_loader.queries as queries


class LoaderConfig:
    """Sets loader running configurations."""

    _factors = [
        "BENCHMARK",
        "BAR",
        "BAR_DELTA",
        "UTILIZATION_PCT",
        "UTILIZATION_PCT_DELTA",
        "LOAN_RATE_AVG",
        "LOAN_RATE_AVG_DELTA",
        "LOAN_RATE_RANGE",
        "LOAN_RATE_RANGE_DELTA",
        "SHORT_INTEREST",
        "SHORT_RATIO",
    ]

    _source_table = {
        "BENCHMARK": "BASE",
        "BAR": "BASE",
        "BAR_DELTA": "METRICS",
        "UTILIZATION_PCT": "BASE",
        "UTILIZATION_PCT_DELTA": "METRICS",
        "LOAN_RATE_AVG": "BASE",
        "LOAN_RATE_AVG_DELTA": "METRICS",
        "LOAN_RATE_RANGE": "BASE",
        "LOAN_RATE_RANGE_DELTA": "METRICS",
        "SHORT_INTEREST": "METRICS",
        "SHORT_RATIO": "METRICS",
    }

    _timeframes = [
        "MONTHLY",
        "WEEKLY",
        "DAILY",
    ]

    def set_configs(self) -> None:
        target = Target(os.environ.get("TARGET"))
        configs = []
        for factor in self._factors:
            for timeframe in self._timeframes:
                configs.append(
                    (
                        factor,
                        timeframe,
                        None,
                        self._source_table[factor],
                    )
                )

        target.execute(queries.ConfigQueries.SET_CONFIGS, configs)
        target.commit_transaction()


config_loader = LoaderConfig()
config_loader.set_configs()
