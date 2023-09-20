{#```doc
description: |
    Drop all temp tables created by the `glueduck_query_to_glue_table` macro
```#}
{% macro glueduck_drop_temp_tables() %}
    {% if execute %}
        {{ modules.dynamic.inc['dbt_glueduck'].drop_temp_tables() }}
    {% endif %}
{% endmacro %}