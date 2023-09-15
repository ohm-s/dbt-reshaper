{#```doc
description: |
    This macro will loop over all SQL files in the project
    and generate docs in the format which DBT expects
    The macros should contain a similar comment block to this one
```#}
{% macro prepare_macro_docs() %}
    {%  if execute %}
        {% do modules.dynamic.inc["reshaper_docs"].extract_dbt_docs(env_var('DBT_MACROS_PATH', '/tmp'))  %}
        {{ print("Macro docs generated. Consider Adding .gitignore for yml files inside your macros folder") }}
    {% endif %}
{% endmacro %}