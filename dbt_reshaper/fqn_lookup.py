from .reshaper_logs import fire_info_event
from dbt.adapters.base import BaseAdapter

def override_schema_lookup(fqn_root):
  def _get_cache_schemas_override(self, manifest):
      fire_info_event('DRI-XI', 'Running overriden schemas to lookup')
      schema_nodes_more = []
      for node in manifest.nodes.values():
          if node.is_relational and not node.is_ephemeral_model:
              if fqn_root in node.fqn[1]:
                  schema_nodes_more.append(self.Relation.create_from(self.config, node).without_identifier())
      list_schema_nodes = set(schema_nodes_more)
      return list_schema_nodes
  # override
  BaseAdapter._get_cache_schemas = _get_cache_schemas_override


