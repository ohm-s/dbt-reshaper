from . import dynamic_modules
from . import project_loader
from . import jinja_docs_include
from . import dbt_runnable_mermaid
from . import model_runner_hooks
from . import source_file_headers
from .fqn_lookup import override_schema_lookup

import os
if 'DBT_RESHAPE_SCHEMA_FQN_ROOT' in os.environ:
  override_schema_lookup(os.environ['DBT_RESHAPE_SCHEMA_FQN_ROOT'])

RESHAPER_LOADED = True