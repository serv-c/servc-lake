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
}

path = "/tmp/delta"


class TestDelta(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.lake = Delta(path)

    @classmethod
    def tearDownClass(cls):
        cls.lake.delete_table(table["name"])
        shutil.rmtree(path)

    def test_create_table(self):
        self.lake.add_table(table)

        metadata = os.listdir(os.path.join(path, table["name"], "_delta_log"))
        self.assertGreater(len(metadata), 0)

        result = self.lake.add_table(table)
        self.assertEqual(result, False)

    def test_write_table(self):
        tbl = self.lake.write(table["name"], df, mode="append")
        self.assertEqual(len(tbl.to_pandas()), 5)

    def test_get_table(self):
        tbl = self.lake.get_table("asdasd")
        self.assertEqual(tbl, None)

        tbl = self.lake.write("asdasd", df)
        self.assertEqual(tbl, None)

    def test_fake_delete(self):
        tbl = self.lake.delete_table("asdasd")
        self.assertEqual(tbl, False)


if __name__ == "__main__":
    unittest.main()
