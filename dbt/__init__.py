from pkgutil import extend_path
import sys

__path__ = extend_path(__path__, __name__)

# Conditionally import Monkey patches to allow custom entrypoints
if 'dbt_reshaper' not in sys.modules:
    from dbt_reshaper import RESHAPER_LOADED