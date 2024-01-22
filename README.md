# servc-lake


## Requirements

As per usual, this package does not come bundled with any libraries to ensure full flexibility on dependencies and security vulnerabilities.

```
$ pip install servc servc-lake [...pandas, pyarrow, etc.]
```

## Documentation

Servc's documentation can be found https://docs.servc.ca


## Delta Lake

### Environment Variables

**DATA_PATH** - the location to start writing files. Default: /tmp/


### Example

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
