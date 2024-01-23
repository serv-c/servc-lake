import os
import shutil
from typing import Any, List, NotRequired, TypedDict, Union

from deltalake import DeltaTable, Schema, write_deltalake
from servc.com.service import ComponentType, ServiceComponent


class Table(TypedDict):
    name: str
    schema: Schema
    partition: Union[List[str], str]
    mode: NotRequired[str]
    description: NotRequired[str]
    custom_metadata: NotRequired[dict]
    otherOptions: NotRequired[Any]


class Delta(ServiceComponent):
    _type: ComponentType = ComponentType.DATABASE

    _dataPath: str = None

    _tables: List[Table] = []

    _deltaTables: List[DeltaTable] = []

    def __init__(self, datapath: str, tables: List[Table] = []):
        super().__init__()
        self._dataPath = datapath
        self._tables = tables

    def _connect(self):
        self.load_tables()
        self.isReady = True
        self.isOpen = True

    def _close(self):
        if self._isOpen:
            self.isReady = False
            self.isOpen = False
            return True
        return False

    def load_tables(self):
        for index, table in enumerate(self._tables):
            if len(self._deltaTables) > index:
                continue

            table_uri = os.path.join(self._dataPath, table["name"])
            if not os.path.exists(table_uri) and table.get("location", "fs") == "fs":
                os.makedirs(table_uri, exist_ok=True)

            delta_table = DeltaTable.create(
                table_uri=table_uri,
                name=table["name"],
                schema=table["schema"],
                partition_by=table["partition"],
                mode=table.get("mode", "ignore"),
                description=table.get("description", None),
                custom_metadata=table.get("custom_metadata", None),
                **table.get("otherOptions", {}),
            )
            self._deltaTables.append(delta_table)

    def add_table(self, table: Table) -> bool:
        if table["name"] not in [t["name"] for t in self._tables]:
            self._tables.append(table)
            self.load_tables()
            return True
        return False

    def get_table(self, tableName: str) -> None | DeltaTable:
        for table in self._deltaTables:
            if table.metadata().name == tableName:
                return table
        return None

    def delete_table(self, tableName: str) -> bool:
        for index, table in enumerate(self._tables):
            if table["name"] == tableName:
                # remove information off lake
                table_uri = os.path.join(self._dataPath, table["name"])
                shutil.rmtree(table_uri)

                del self._tables[index]
                del self._deltaTables[index]
                return True
        return False

    def write(
        self, tableName: str, data: Any, mode: str = "append", **other
    ) -> DeltaTable | None:
        table = self.get_table(tableName)
        if table:
            write_deltalake(table, data, mode=mode, **other)
            return table
        return None
