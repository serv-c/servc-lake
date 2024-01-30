import os
from typing import List

import pyarrow as pa
import pyarrow.parquet as pq

from servclake import TableFile


class Parquet(TableFile):
    def read(self, partition: List[str] = [], columns=None) -> pa.Table:
        path = self.get_path(partition)
        if not os.path.exists(path):
            table = pa.Table.from_pylist([], schema=self._schema)
            if self.check_part(partition):
                self.write(table, partition)
            else:
                return table

        return pq.read_table(path, schema=self._schema, columns=columns)

    def write(self, df: pa.Table, partition: List[str] = []) -> pa.Table:
        if not self.check_part(partition):
            partitions = self.get_all_partitions(df)
            for part, sub_df in partitions:
                if part[: len(partition)] == partition:
                    self.write(sub_df, part)
            return df

        path = self.get_path(partition)
        dirname = os.path.dirname(path)
        if not os.path.exists(dirname):
            os.makedirs(dirname, exist_ok=True)

        pq.write_table(df, path)
        return df
