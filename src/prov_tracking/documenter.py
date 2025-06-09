from typing import Any
from dask.task_spec import DataNode, Task, Alias
from dask.typing import Key
from distributed.scheduler import TaskState

import prov.model as prov
from prov_tracking.utils import RunnableTaskInfo, GeneratedValue, ReadyValue, Value

def _sanitize(string: str) -> str:
  """Given a string, returns a new string without `(`, `)`, `\\` and with
  `,` substituted by `_`."""

  return string.replace('(', '').replace(')', '').replace('\'', '').replace(', ', '_')

def _serialize_value(obj: Any) -> str:
  """Ginen an object, serializes it as a string."""

  if isinstance(obj, str):
    return obj
  elif isinstance(obj, (list, tuple)):
    tmp_list = [ _serialize_value(x) for x in obj ]
    if isinstance(obj, tuple):
      return str(tuple(tmp_list))
    else:
      return str(tmp_list)
  elif isinstance(obj, dict):
    return str({ k: _serialize_value(v) for k, v in obj.items() })
  else:
    return str(obj)

def _type(obj: Any) -> str:
  """Given and object, returns a string representing its type. The string is
  richer than that produced by simply calling `str(type(obj))`."""

  if isinstance(obj, list):
    if len(obj) == 0:
      return 'list[()]'
    else:
      return f'list[{_type(obj[0])}]'
  if isinstance(obj, tuple):
    if len(obj) == 0:
      return 'tuple[()]'
    else:
      types = []
      for item in obj:
        types.append(_type(item))
      return f'tuple[{', '.join(types)}]'
  if not isinstance(obj, type):
    obj = type(obj)
  module = obj.__module__
  if module != 'builtins':
    return f'{module}.{obj.__qualname__}'
  else:
    return obj.__qualname__
    

class Documenter:
  """Handles the creation of entities and activities associated with tasks."""

  def __init__(self, **kwargs):
    """Additional arguments:
    - `destination: str`: where to save the serialized provenance document.
    Defaults to `./provenance.json`
    - `format: str`: serialization format for the document. Possible values are
    `json`, `xml`, `rdf`, `provn`. Defaults to `json`
    - `rich_types: bool`: tells if datatypes of values such be richer, e.g. for
    tuples, track the type of each element instead of just saying that the value
    is a tuple. Defaults to `False`
    """

    self.document = prov.ProvDocument()
    self.document.set_default_namespace('dask-prov.dict')
    self.format: str = kwargs.pop('format', 'json')
    self.destination: str = kwargs.pop('destination', './provenance.json')
    self.rich_types: bool = kwargs.pop('rich_types', False)
    self.kwargs: dict[str, Any] = kwargs

  def register_non_runnable_task(self, task_id: Key, task: DataNode):
    """Non-runnable tasks are registered as entities as they are in fact just data"""

    key = _sanitize(str(task_id))
    self.document.entity(
      identifier=key,
      other_attributes={
        'value': _serialize_value(task.value),
        'dtype': str(type(task.typ)) if not self.rich_types else _type(task.typ)
      }
    )

  def register_task_param(self, activity_id: str, name: str, param: Value):
    """Registers the param name for activity `activity_id` according to its
    value. If it is a `ReadyValue`, an entity is created for the parameter and
    the pair `(name, key)` is returned, with `key` being the identifier of the
    created entity. If instead param is a `GeneratedValue`, the returned pair
    has the identifier of the entity that represents the return value of the
    generator activity.
    """
    key = None
    if isinstance(param, ReadyValue):
      key = param.key
    elif isinstance(param, GeneratedValue):
      key = f'{_sanitize(param.generatedBy)}.return_value'
    else:
      key = f'{activity_id}.{name}'
      self.document.entity(
        identifier=key,
        other_attributes={
          'value': _serialize_value(param.value),
          'dtype': str(type(param.value)) if not self.rich_types else _type(param.value)
        }
      )
      
    return (name, key)

  def register_runnable_task(self, info: RunnableTaskInfo) -> str:
    """Runnble tasks are registered as activities. Parameters to the task are
    registered as entities linked to the activity via used relations. All
    runnable dependencies of the task are registered via communication relations."""

    activity_id = _sanitize(str(info.key))
    attributes = {
      'name': str(info.func),
      'module': info.func.__module__,
      'group': info.group,
    }
    if hasattr(info.func, '__name__'):
      attributes['nice_name'] = info.func.__name__

    self.document.activity(
      identifier=activity_id,
      startTime=info.start_time,
      endTime=info.finish_time,
      other_attributes=attributes
    )

    # Records the used parameters
    used_params = []
    for name, param in info.args_dict.items():
      if not isinstance(param, set):
        used_params.append(self.register_task_param(activity_id, name, param))
      else:
        for value in param:
          used_params.append(self.register_task_param(activity_id, name, value))

    for name, key in used_params:
      self.document.used(
        activity=activity_id,
        entity=key,
        time=info.start_time,
        other_attributes={
          'as_parameter': name
        }
      )
    
    for informant in info.informants:
      key = _sanitize(str(informant))
      self.document.wasInformedBy(informed=activity_id, informant=key)

    return activity_id

  def register_successful_task(
    self, info: RunnableTaskInfo, dtype: str | None, nbytes: int | None
  ):
    """Registers the successful completion of a runnble tasks and the value that
    the task produced."""

    activity_id = self.register_runnable_task(info)

    # Records the value generated by this funcion
    key = f'{activity_id}.return_value'
    other_attributes = {}
    if dtype is not None and nbytes is not None:
      other_attributes = {
      'dtype': dtype,
      'nbytes': str(nbytes)
    }
    self.document.entity(
      identifier=key,
      other_attributes=other_attributes
    )
    self.document.wasGeneratedBy(
      entity=key,
      activity=activity_id,
      time=info.finish_time
    )

  def register_failed_task(
    self, info: RunnableTaskInfo, exception_text: str | None,
    traceback: str | None, blamed_task: TaskState | None
  ):
    """Registers the failed completion of some runnble tasks and records the
    exception they raised."""

    activity_id = self.register_runnable_task(info)
    key = f'{activity_id}.return_value'
    attributes: dict[str, Any] = {
      'is_error': True,
    }
    if exception_text is not None:
      attributes['exception_text'] = exception_text
    
      if traceback is not None:
        attributes['traceback'] = traceback
      if blamed_task is not None and blamed_task.key != activity_id:
        other_task_id = _sanitize(str(blamed_task.key))
        attributes['blamed_task'] = other_task_id
        self.document.wasInformedBy(informed=activity_id, informant=other_task_id)

    self.document.entity(
      identifier=key,
      other_attributes=attributes
    )
    self.document.wasGeneratedBy(
      entity=key, activity=activity_id, time=info.finish_time
    )

  def serialize(self, destination=None, format=None, **kwargs: dict[str, Any]):
    """
    Serializes the provenance document into `destination`, or returns the
    serialized string if no destination was provided. The format used is `format`,
    or the default format provided on initialization if the parameter here is
    `None`.
    """

    if format is None:
      format = self.format
    if destination is None and self.destination is not None:
      destination = self.destination
    if len(kwargs) == 0:
      kwargs = self.kwargs

    return self.document.serialize(
      destination=destination, format=format, **kwargs
    )
