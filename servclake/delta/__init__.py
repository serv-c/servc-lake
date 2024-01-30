import os
from typing import Dict, List

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from servclake import Table, TableFile


class Delta(TableFile):
    _otherOptions: Dict = None

    _deltaTable: DeltaTable = None

    def __init__(self, table: Table, datapath: str = None, otherOptions: Dict = {}):
        super().__init__(table, datapath)
        self._otherOptions = otherOptions

        table_uri = self.get_path()
        if not os.path.exists(table_uri) and table.get("location", "fs") == "fs":
            os.makedirs(table_uri, exist_ok=True)
        self._deltaTable = DeltaTable.create(
            table_uri=table_uri,
            name=self._name,
            schema=self._schema,
            partition_by=self._partition,
            **self._otherOptions,
        )

    def get_path(self) -> str:
        return self._dataPath

    def read(self, partition: List[str] = [], **other) -> pa.Table:
        partition_filter = []
        for index, value in enumerate(partition):
            partition_filter.append((self._partition[index], "=", value))
        return self._deltaTable.to_pyarrow_dataset(
            partitions=partition_filter
        ).to_table(**other)

    def write(self, df: pa.Table, **other) -> pa.Table:
        write_deltalake(self._deltaTable, df, **other)
        return df

    def append(self, df: pa.Table, **other) -> pa.Table:
        return self.write(df, mode="append", **other)
