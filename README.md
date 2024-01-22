# servc-lake


## Requirements

As per usual, this package does not come bundled with any libraries to ensure full flexibility on dependencies and security vulnerabilities.

```
$ pip install servc [...lake libraries]
```

## Documentation

Servc's documentation can be found https://docs.servc.ca


## Delta Lake

### Environment Variables

**DATA_PATH** - the location to start writing files. Default: /tmp/


### Example

Here is the most simple example of use, 

```python
from servc.com.server.server import start_server

def inputProcessor(messageId, bus, cache, components, message, emit):
  pass

# the method 'methodA' will be resolved by inputProcessor
start_server(
  "my-route",
  {
    "methodA": inputProcessor
  }
)
```
