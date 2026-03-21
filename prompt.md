We're creating a repo to manipulate popular macromolecular crystallography data file formats.
Long term, it will support data from multiple applications, but for now we will focus on `.stream` files
from CrystFEL. This repo will contain an API for interacting with stream files and command line
tools as well for creating, modifying, and splitting files. 

Let's focus on building the first version which needs the following features. 
1) a pyproject.toml file so it can be pip installed
2) unit tests and end-to-end test for the CLI
3) an API for loading, modifying, and writing stream files
4) a CLI to split a stream file by chunk into different classes

### More on the pyproject.toml (1)
Use setuptools and pytest for testing. No fancy project management tools like hatch or tox. 
Use GitHub actions for CI.

### More on the API (3)
core classes

```
class Stream(file_name):
    def filter(self, func): -> stream
        """
        Filter chunks from a stream file. If func(self, chunk) returns True, include it in the output.
        """
        ...

    def write(self, file_name): 
        ...

    def __iter__(self) -> Sequence(Chunk):
        """
        Yield one chunk at a time
        """

class Chunk(lines):
    """
    A class organizing the contents of a chunk with attributes for important metadata.
    """
```

Key design goals
 - reduce memory footprint by lazily loading chunks as needed by other methods. use mmap and seek to navigate the file efficiently. 
 - use regular expressions to quickly find the chunk boundaries
 - use python multiprocessing to speed up data loading and filtering


### More on the CLI (4)
This CLI will be used to filter data from serial crystallography experiments into different classes based on the content of each chunk.
Write a CLI using argparse to accomplish this task. A rough outline for how this should work is

`split_stream stream_file --event-codes=40,41 --labels=Dark,Light`

This command should output two stream files containing the same header but different chunks corresponding to each event code. 
We don't yet know if the event codes will appear in the chunk metadata or if we will have to query an external database to discover them.
For now, you can just implement everything else except for the filter function which will be used to determine whether a chunk matches
a given event code. 

