# servc-lake


## Requirements

As per usual, this package does not come bundled with any libraries to ensure full flexibility on dependencies and security vulnerabilities.

```
$ pip install servc servc-lake pyarrow [pandas]
```

## Environment Variables

**DATA_PATH** - the location to start writing files. Default: /tmp/datalake

## Parquet

```python
import pyarrow as pa
from servclake.parquet import Parquet

table = Parquet(
  {
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
)

# write data to table
table.write(pa.Table.from_pylist([{"col1": "a", "col2": 1, "j3": 1}]))

# read partition. reads partiion 1 of col2.
df = table.read([1])
```

## Delta Lake

```python
import pyarrow as pa
from servclake.delta import Delta

# create a deltalake instance
deltalake = Delta(
  "/datalake",
  [
    {
      "name": "mytesttable",
      "schema": pa.schema([pa.field("col", pa.string()), pa.field("value", pa.int32())]),
      "partition": "col",
    }
  ]
)

# delcare our dataframe
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

# write dataframe to lake
table = self.lake.write("mytesttable", df, mode="append")
print(table.to_pandas())
```
