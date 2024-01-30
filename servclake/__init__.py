import hashlib
import os
from typing import List, TypedDict

import pyarrow as pa
import pyarrow.compute as pc
from servc.com.service import ComponentType, ServiceComponent

from servclake.config import DATA_PATH


class Table(TypedDict):
    name: str
    schema: pa.Schema
    partition: List[str]


class TableFile(ServiceComponent):
    _type: ComponentType = ComponentType.DATABASE

    _dataPath: str = None

    _name: str = None

    _schema: pa.Schema = None

    _partition: List[str] = None

    def __init__(self, table: Table, datapath: str = None):
        super().__init__()
        self._name = table["name"]
        self._schema = table["schema"]
        self._partition = table["partition"]

        if datapath is None:
            datapath = DATA_PATH
        self._dataPath = os.path.join(datapath, self._name)

    def _connect(self):
        self.isReady = True
        self.isOpen = True

    def _close(self):
        if self._isOpen:
            self.isReady = False
            self.isOpen = False
            return True
        return False

    def get_path(self, partition: List[str]) -> str:
        path = self._dataPath
        for index, value in enumerate(partition):
            key = self._partition[index]
            path = os.path.join(path, "=".join([key, str(value)]))
        if self.check_part(partition):
            hash = hashlib.sha1(path.encode("utf-8")).hexdigest() + ".parquet"
            path = os.path.join(path, hash)

        return path

    def check_part(self, partition: List[str], raise_exception: bool = None) -> bool:
        if raise_exception is None:
            raise_exception = False
        if len(partition) != len(self._partition):
            if raise_exception:
                raise ValueError(
                    f"Partition length {len(partition)} does not match table partition length {len(self._partition)}"
                )
            return False
        return True

    def get_all_partitions(
        self, df: pa.Table, node=None, haystack=None
    ) -> List[tuple[List[str], pa.Table]]:
        if node is None:
            node = 0
            haystack = []
        partitions = []
        column = pc.field(self._partition[node])

        for needle in df.column(self._partition[node]).to_pylist():
            expr = column == needle
            sub_df = df.filter(expr)

            partition = [*haystack, needle]
            if node + 1 == len(self._partition):
                partitions.append((partition, sub_df))
            else:
                partitions.extend(self.get_all_partitions(sub_df, node + 1, partition))

        return partitions

    def read(self, partition: List[str] = [], columns: List[str] = None) -> pa.Table:
        pass

    def write(self, df: pa.Table, partition: List[str] = []) -> pa.Table:
        pass

    def append(self, df: pa.Table, partition: List[str] = []) -> pa.Table:
        existing_df = self.read(partition)
        new_df = pa.concat_tables([existing_df, df])
        self.write(new_df, partition)

        return new_df
