{% macro generate_schema_name(custom_schema_name, node) -%}

    {% set base_schema = custom_schema_name | trim %}
    {{ return(base_schema) }}

{%- endmacro %}