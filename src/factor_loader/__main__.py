from datetime import datetime
import logging
from sys import stdout
import os
from typing import Dict, List, Union

from factor_loader.date_helpers import generate_intervals
import factor_loader.model as model
import factor_loader.queries as queries
from factor_loader.persistence import source, target

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s [%(filename)s:%(lineno)d]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=stdout,
)

logger = logging.getLogger(__name__)


class Loader:
    """Loader class for factor returns."""

    YEARS = [
        2010,
        2011,
        2012,
        2013,
        2014,
        2015,
        2016,
        2017,
        2018,
        2019,
        2020,
        2021,
        2022,
        2023,
    ]

    _mkt_cap_ranges = {
        "small": (0, 500),
        "mid": (500, 10_000),
        "large": (10_000, 999_999_999),
    }

    _selection_amounts = [20, 50, 100]

    source: source.Source
    target: target.Target
    configs: List[model.Config]

    def __init__(self) -> None:
        self.source = source.Source(os.environ.get("SOURCE"))
        self.target = target.Target(os.environ.get("TARGET"))
        self.timeframe = os.environ.get("TIMEFRAME")

        self.configs = self.set_configs()

    def set_configs(self):
        raw_configs = self.source.fetch_configs()
        configs = [model.Config.build_record(c) for c in raw_configs]
        configs = [c for c in configs if c.timeframe == self.timeframe]
        return configs

    def run(self):
        logger.info("Starting process...")

        date_ranges = generate_intervals(self.YEARS)
        last_persisted_date = self.target.fetch_last_date_persisted(self.timeframe)
        if last_persisted_date:
            date_ranges = [dr for dr in date_ranges if dr[0] >= last_persisted_date]

        history: Dict[
            datetime, Union[List[model.BaseData], List[model.MetricsData]]
        ] = {}
        max_date = None

        n = len(date_ranges)
        i = 0
        for date_range in date_ranges:
            logger.info(f"Persisted {i}/{n} date ranges.")
            logger.info(f"Processing records from {date_range[1].year}...")
            m = len(self.configs)
            j = 0
            prev_config = None
            for config in self.configs:
                logger.info(f"Processed {j}/{m} configs.")
                logger.info(f"Processing {config}...")
                # ONLY FETCHES RECORDS AGAIN IF NEEDED
                if (
                    not prev_config
                    or config.timeframe != prev_config.timeframe
                    or config.source_table != prev_config.source_table
                ):
                    history = self.build_history(date_range, config)

                if history:
                    max_date = max(history.keys())

                    records = self.run_config(history, config)

                    config_record = [
                        (config.factor.upper(), config.timeframe.upper(), max_date, config.source_table.upper())
                    ]

                    self.target.execute(queries.ConfigQueries.UPSERT, config_record)
                    self.target.execute(queries.FactorReturnsQueries.UPSERT, records)
                    self.target.commit_transaction()
                else:
                    logger.info("No records left to process for this config.")
                prev_config = config
                j += 1

            if max_date:
                history = {max_date: history[max_date]}

            logger.info(f"Persisted {date_range} for every config.")
            i += 1

        logger.info("Process finished.")

    def build_history(self, date_range, config):
        history: Dict[
            datetime, Union[List[model.BaseData], List[model.MetricsData]]
        ] = {}

        logger.debug("Fetching records...")
        raw_records = self.source.get_records(
            timeframe=config.timeframe,
            source_table=config.source_table,
            date_range=date_range,
        )
        if raw_records:
            logger.debug("Curating records...")
            curated_records = None
            if config.source_table == "base":
                curated_records = [
                    model.BaseData.build_record(r)
                    for r in raw_records
                ]
            if config.source_table == "metrics":
                curated_records = [
                    model.MetricsData.build_record(r)
                    for r in raw_records
                ]

            if curated_records:
                logger.debug("Building history per date...")
                for record in curated_records:
                    if record.datadate not in history.keys():
                        history[record.datadate] = [record]
                    else:
                        history[record.datadate].append(record)

        return history

    def run_config(
        self,
        history: Dict[datetime, Union[List[model.BaseData], List[model.MetricsData]]],
        config: model.Config,
    ):
        """Runs a configuration through the provided history."""
        dates = list(history.keys())
        dates.sort()

        res = []
        for i in range(0, len(dates) - 1):
            curr_records = history[dates[i]]
            next_date = dates[i + 1]
            next_records = history[next_date]
            logger.debug("Sorting factor...")
            sorted_factor = self.sort_factor(config.factor, curr_records)
            logger.debug("Getting returns...")
            returns_dict = self.get_top_flop(sorted_factor, next_records)
            logger.debug("Computing return...")
            records = self.compute_returns(next_date, config, returns_dict)
            res.extend(records)

        return res

    def sort_factor(self, factor, records):
        # LOOP THROUGH THE MARKET CAP RANGES DICT.
        # INPUT LIST OF CURR RECORDS AND FACTOR
        # RETURN DICT WITH MKT CAP CLASS AS KEY AND SORTED RECORDS AS VALUE.
        res = {}
        for mkt_cap_class, mkt_cap_range in self._mkt_cap_ranges.items():
            min_mkt_cap = mkt_cap_range[0]
            max_mkt_cap = mkt_cap_range[1]

            filtered_records = [
                r
                for r in records
                if getattr(r, factor) is not None
                and min_mkt_cap < getattr(r, "market_cap") <= max_mkt_cap
            ]
            filtered_records.sort(key=lambda x: getattr(x, factor))

            res[mkt_cap_class] = filtered_records

        return res

    def get_top_flop(self, sorted_factor, rtn_records):
        # LOOP THROUGH THE SELECTION AMOUNT DICT.
        # INPUT MKT CP DICT AND NEXT RECORDS
        # RETURN NESTED DICT: MKT CAP -> SELECTION AMOUNT -> TOP/FLOP/CONSISTENT/GVKEYS
        res = {}
        rtn_keys = [r.gvkey for r in rtn_records]
        for mkt_cap_class, factor_records in sorted_factor.items():
            for selection_amount in self._selection_amounts:

                records_with_returns = [r for r in factor_records if r.gvkey in rtn_keys]

                top_keys = [r.gvkey for r in records_with_returns[-selection_amount:]]
                flop_keys = [r.gvkey for r in records_with_returns[:selection_amount]]
                gvkeys = {
                    "LONG": flop_keys,
                    "SHORT": top_keys
                }

                top_returns = [r.winsorized_5_rtn for r in rtn_records if r.gvkey in top_keys]
                flop_returns = [r.winsorized_5_rtn for r in rtn_records if r.gvkey in flop_keys]

                if len(records_with_returns) >= selection_amount * 2:
                    consistent = True
                else:
                    consistent = False

                if mkt_cap_class not in res.keys():
                    res[mkt_cap_class] = {}

                res[mkt_cap_class][selection_amount] = {}
                res[mkt_cap_class][selection_amount]["top"] = top_returns
                res[mkt_cap_class][selection_amount]["flop"] = flop_returns
                res[mkt_cap_class][selection_amount]["consistent"] = consistent
                res[mkt_cap_class][selection_amount]["gvkeys"] = gvkeys

        return res

    @staticmethod
    def compute_returns(next_date: datetime, config: model.Config, returns_dict: Dict):
        """Computes returns using dict output from get_top_flop."""
        res = []
        for mkt_cap_class, selection_amount_dict in returns_dict.items():
            for selection_amount, portfolio in selection_amount_dict.items():
                flop_returns = portfolio["flop"]
                top_returns = portfolio["top"]
                consistent = portfolio["consistent"]
                gvkeys = portfolio["gvkeys"]

                long_returns = (
                    sum(flop_returns) / len(flop_returns) if flop_returns else None
                )
                short_returns = (
                    sum([-r for r in top_returns]) / len(top_returns)
                    if top_returns
                    else None
                )

                returns = (
                    (long_returns + short_returns) / 2
                    if short_returns and long_returns
                    else None
                )
                record = (
                    next_date,
                    config.factor.upper(),
                    config.timeframe.upper(),
                    mkt_cap_class.upper(),
                    selection_amount,
                    long_returns,
                    short_returns,
                    returns,
                    consistent,
                    gvkeys,
                )

                res.append(model.FactorReturns.build_record(record).as_tuple())

        return res


loader = Loader()
loader.run()
