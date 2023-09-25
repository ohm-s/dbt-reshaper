import base64
import json
from typing import Sequence

from dbt.config import RuntimeConfig
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.results import RunResult
from dbt.task.runnable import GraphRunnableTask
from dbt.adapters.factory import get_adapter
from .reshaper_logs import fire_info_event,fire_error_event
from .dynamic_modules import DynamicModules
from dbt.contracts.graph.parsed import ParsedModelNode
from typing import MutableMapping
from dbt.task.run import RunTask

_original_run = GraphRunnableTask.run

def run_with_mermaid(self: GraphRunnableTask):
  adapter_type = get_adapter(self.config).type()
  if adapter_type in DynamicModules.adapter_patches:
    for patch in DynamicModules.adapter_patches[adapter_type]:
      patch(self)

  result = _original_run(self)

  if isinstance(self, RunTask):
    invoke_urls(result.results, self.manifest, self.config)

  try:
    generate_mermaid_graph(result.results, self.manifest, self.config)
  except Exception as e:
    fire_error_event("DRE-XX", "MERMAID GRAPH ERROR: " + str(e))

  return result


def generate_mermaid_graph(results: Sequence[RunResult], manifest: Manifest, config: RuntimeConfig):
  fire_info_event("DR-XX", "Generating mermaid graph")
  remove_prefix = 'model.' +config.project_name + '.'
  affected_models = []
  for result in results:
      affected_models.append(result.node.unique_id)

  if manifest is not None and len(manifest.nodes) > 0:
      nodes = manifest.nodes
      models: MutableMapping[str, ParsedModelNode] = {k: v for k, v in nodes.items() if v.resource_type == 'model'}

      # get all models which are affected
      selected_models = {}
      for key in models:
          model = models[key]
          u = model.unique_id
          if u in affected_models:
              selected_models[u] = model

      # build a graph of nodes based on unique_id and depends_on.nodes
      dependency_graph = {}
      for k in selected_models:
          model = selected_models[k]

          dependency_graph[model.unique_id] = all_dependencies = model.depends_on.nodes

      relevant_dependencies = dependency_graph.keys()
      relevant_graph = {}
      # intersect all dependencies with the selected models
      all_root_nodes = []
      for k in dependency_graph:
          dependencies = dependency_graph[k]
          # only keep dependencies which are in relevant_dependencies
          shortlist = [d for d in dependencies if d in relevant_dependencies]
          relevant_graph[k] = shortlist
          if len(shortlist) == 0:
              all_root_nodes.append(k)

      #print(json.dumps(relevant_graph, indent=4)  )
      mermaid_code = "graph LR;\nstart[Start];\n"

      # Add nodes
      for key in relevant_graph.keys():
          mermaid_code += key.replace(remove_prefix, '') + ";\n"
          if key in all_root_nodes:
              mermaid_code += "start-->" + key.replace(remove_prefix, '') + ";\n"

      # Add edges
      for key, value in relevant_graph.items():
          if isinstance(value, list):
              for item in value:
                  mermaid_code += item.replace(remove_prefix, '') + "-->" + key.replace(remove_prefix, '') + ";\n"


      obj = {"code": mermaid_code, "mermaid":"{\n  \"theme\": \"default\"\n}"}
      base64_obj = base64.b64encode(json.dumps(obj).encode('utf-8'))
      url = "https://mermaid.live/edit/#base64:" + base64_obj.decode('utf-8')
      fire_info_event("DRI-XX", url)


def invoke_urls(results: Sequence[RunResult], manifest: Manifest, config: RuntimeConfig):
  fire_info_event("DR-XX", "Figuring out if we have invokable urls defined in models")
  affected_models = []
  successful_models = []
  for result in results:
      affected_models.append(result.node.unique_id)
      if result.status == 'success':
        successful_models.append(result.node.unique_id)

  if manifest is not None and len(manifest.nodes) > 0:
      nodes = manifest.nodes
      models: MutableMapping[str, ParsedModelNode] = {k: v for k, v in nodes.items() if v.resource_type == 'model'}

      # get all models which are affected
      selected_models = {}
      for key in models:
          model = models[key]
          u = model.unique_id
          if u in affected_models:
              selected_models[u] = model
              if 'dbt:invoke_url' in model.meta:
                  url = model.meta['dbt:invoke_url']
                  if model.unique_id in successful_models:
                      fire_info_event("DR-XX", "Model " + model.unique_id + " was successful, invoking url: " + url)

GraphRunnableTask.run = run_with_mermaid

