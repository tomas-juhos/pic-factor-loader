"""Source."""

from typing import List, Tuple

import psycopg2
import psycopg2.extensions


class Source:
    """Source class."""

    def __init__(self, connection_string: str) -> None:
        self._connection_string = connection_string
        self._connection = psycopg2.connect(connection_string)
        self._connection.autocommit = False
        self._tx_cursor = None

    @property
    def cursor(self) -> psycopg2.extensions.cursor:
        """Generate cursor.

        Returns:
            Cursor.
        """
        if self._tx_cursor is not None:
            cursor = self._tx_cursor
        else:
            cursor = self._connection.cursor()

        return cursor

    def disconnect(self) -> None:
        """Disconnect from database."""
        self._connection.close()

    def fetch_configs(self):
        """Fetches every run configuration."""
        cursor = self.cursor
        query = (
            "SELECT * FROM factor_loader_config flc "
            "ORDER BY flc.source_table, flc.factor;"
        )
        cursor.execute(query)
        configs = cursor.fetchall()

        return configs

    def fetch_us_keys(self):
        """Fetches U.S. gvkeys."""
        cursor = self.cursor
        query = "SELECT DISTINCT gvkey " "FROM country c " "WHERE c.country = 'USA'; "
        cursor.execute(query)
        keys = cursor.fetchall()

        return [k[0] for k in keys] if keys else None

    def get_records(self, timeframe, source_table, date_range) -> List[Tuple]:
        """Fetch records with the provided keys.

        Args:
            timeframe: timeframe.
            source_table: Source table.
            date_range: date range to get records from.

        Returns:
            List of records with matching keys.
        """
        cursor = self.cursor
        query = (
            "SELECT * "
            f"FROM {timeframe}_{source_table} "
            "WHERE datadate between %s and %s; "
        ).format(timeframe=timeframe, source_table=source_table)

        cursor.execute(query, (date_range[0], date_range[1]))
        res = cursor.fetchall()

        return res if res else None
