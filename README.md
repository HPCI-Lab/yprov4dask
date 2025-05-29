# yprov4dask
A plugin that enables provenance tracking in Dask

## Environment setup
Dowload the repo from `https://github.com/HPCI-Lab/yprov4dask` and install the
plugin in your python environment by running `pip install .` from within the
root folder of the repository.

### Note for developers
Install the package in development mode using `pip install -e .` if you want
new modifications to the code to be reflected immediately in your environment.
If the `-e` option is missing, you will have to reinstall the plugin every time
you modify it.

## Usage
To use the plugin, simply import it into your code, instantiate the plugin and
register it with your Dask client.

### Example
```python
from dask.distributed import Client
from prov_tracking import ProvTracker

# Can be avoided if working with Jupyter notebooks
if __name__ == '__main__':
    # Create you Dask client with any client options you want
    client = Client()
    # The plugin creation is as simple as this, no parameter is required
    plugin = ProvTracker()
    # The plugin requires a reference to the scheduler, but you must register
    # it with the client before providing it
    client.register_plugin(plugin)
    plugin.start(client.scheduler)

    # Your analysis...

    # When the client is closed the provenance document is automatically generated
    client.close()
```

### Note
The plugin can only track what comes through the Dask scheduler, so if you're
computations are not translated in Dask tasks, you won't see anything in your
provenance document. For example, if you open a dataset with `xarray` and you
want to track its provenance, always make sure that it is using a `DaskArray`
under the hood. If you use `xr.open_dataset`, you can be ensure that by
providing some value for the `chunks` argument. Even `chunks={}` is fine, even
tho that may produce a really inefficient arrangment.

### Additional options
Upon plugin initialization you can provide the following options:
- `destination`: path for the provenance document. Defaults to `'provenance.json'`;
- `format`: format for the provenance document. Can be `'json'`, `'xml'`,
`'rdf'`, `'provn'` and defaults to `'json'`;
- `keep_traceback`: as the plugin registers exceptions when these are raised by
some task, this parameter, if set to `True` the traceback text of the exception
is saved in the provenance document. Defaults to `False`;
- `rich_types`: as the plugin registers the data type of all parameters and
return values of tasks, if this is set to `True`, the type information is richer.
Defaults to `False`;

You can also provide all kwargs accepted by `prov.model.ProvDocument.serialize`.
For instance, `indent` if provided with an interger value allows the generation
of more human-readable documents with lines indented according to the parameter.
