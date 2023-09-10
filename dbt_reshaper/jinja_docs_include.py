from dbt.clients import jinja
from .dynamic_modules import DynamicModules

def get_evironment_with_include_support(
    node=None,
    capture_macros: bool = False,
    native: bool = False,
):
    #print('override jinja')
    args = {
        "extensions": ["jinja2.ext.do"]
    }
    if capture_macros:
        args["undefined"] = jinja.create_undefined(node)

    args["extensions"].append(jinja.MaterializationExtension)
    args["extensions"].append(jinja.DocumentationExtension)
    args["extensions"].append(jinja.TestExtension)
    from jinja2 import FileSystemLoader

    env_cls = None

    if native:
        env_cls = jinja.NativeSandboxEnvironment
        filters = jinja.NATIVE_FILTERS
    else:
        env_cls = jinja.MacroFuzzEnvironment
        filters = jinja.TEXT_FILTERS

    env = env_cls(**args)
    if len(DynamicModules.docs_include_paths) > 0:
      env.loader = FileSystemLoader(DynamicModules.docs_include_paths)
    env.filters.update(filters)

    return env

jinja.get_environment = get_evironment_with_include_support