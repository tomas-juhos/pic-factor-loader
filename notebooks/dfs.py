from helpers import Source


class Dataframes:
    source = Source("local_data")

    bse = source.get_df("best_ever_strategy_short_returns.parquet")
