{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    
    {# Nếu không khai báo schema riêng, dùng schema mặc định #}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}

    {# Nếu có khai báo, dùng CHÍNH XÁC tên được khai báo (không ghép nối) #}
    {%- else -%}
        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}