import os
import shutil
import unittest

import pyarrow as pa

from servclake.delta import Delta

df = pa.Table.from_pylist(
    [
        {"col": "df1", "value": 1},
        {"col": "asd2", "value": 2},
        {"col": "asd3", "value": 3},
        {"col": "asd4", "value": 4},
        {"col": "asd5", "value": 5},
    ],
    schema=pa.schema([pa.field("col", pa.string()), pa.field("value", pa.int32())]),
)

table = {
    "name": "mytesttable",
    "schema": pa.schema([pa.field("col", pa.string()), pa.field("value", pa.int32())]),
    "partition": "col",
    "location": "fs",
}

path = "/tmp/delta"


class TestDelta(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.path.exists(path):
            shutil.rmtree(path)
        cls.lake = Delta(table, path)

    def test_create_table(self):
        metadata = os.listdir(os.path.join(path, table["name"], "_delta_log"))
        self.assertGreater(len(metadata), 0)

    def test_write_table(self):
        tbl = self.lake.append(df)
        self.assertEqual(len(tbl.to_pandas()), 5)
        self.assertEqual(len(self.lake.read()), 5)


if __name__ == "__main__":
    unittest.main()
