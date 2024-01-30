import os
import shutil
import unittest

import pyarrow as pa

from servclake import Table
from servclake.parquet import Parquet

table: Table = {
    "name": "mytesttable",
    "schema": pa.schema(
        [
            pa.field("col1", pa.string(), nullable=False),
            pa.field("col2", pa.uint32(), nullable=False),
            pa.field("j3", pa.uint32(), nullable=False),
        ]
    ),
    "partition": ["col2", "col1"],
}
data_path = "/tmp/datalake/parquet"

df = pa.Table.from_pydict(
    {
        "col1": ["a", "b", "c", "b"],
        "col2": [1, 2, 3, 1],
        "j3": [1, 2, 3, 4],
    },
    schema=table["schema"],
)


class TestParQuet(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.table = Parquet(table, data_path)

    def setUp(self):
        shutil.rmtree(data_path, ignore_errors=True)
        self.table.write(df)

    def test_append_partition(self):
        df_append = pa.Table.from_pydict(
            {
                "col1": ["x", "b"],
                "col2": [0, 2],
                "j3": [3, 2],
            },
            schema=table["schema"],
        )

        # append the new data
        self.assertEqual(len(self.table.read(["x"])), 0)
        self.table.append(df_append, [0])
        self.assertEqual(len(self.table.read(["0"])), 1)

        # ensure we didnt loose the old data
        self.assertEqual(len(self.table.read(["1", "b"])), 1)
        self.assertEqual(len(self.table.read(["2", "b"])), 1)

    def test_append_table(self):
        df_append = pa.Table.from_pydict(
            {
                "col1": ["x", "b"],
                "col2": [0, 2],
                "j3": [3, 2],
            },
            schema=table["schema"],
        )

        # append the new data
        self.assertEqual(len(self.table.read(["x"])), 0)
        self.table.append(df_append)
        self.assertEqual(len(self.table.read(["0"])), 1)

        # ensure we didnt loose the old data
        self.assertEqual(len(self.table.read(["1", "b"])), 1)
        self.assertEqual(len(self.table.read(["2", "b"])), 2)

    def test_writing_table(self):
        self.assertTrue(
            os.path.exists(os.path.join(data_path, table["name"], "col2=1"))
        )

        self.assertTrue(
            os.path.exists(
                os.path.join(
                    data_path,
                    table["name"],
                    "col2=1",
                    "col1=a",
                )
            )
        )

        self.assertTrue(
            os.path.exists(os.path.join(data_path, table["name"], "col2=3"))
        )

    def test_read_table(self):
        table = self.table.read()
        self.assertEqual(len(table), 4)

    def test_read_partition(self):
        table = self.table.read(["1"])
        self.assertEqual(len(table), 2)

        table = self.table.read(["1", "b"])
        self.assertEqual(len(table), 1)


if __name__ == "__main__":
    unittest.main()
