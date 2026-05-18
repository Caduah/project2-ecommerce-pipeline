-- macros/incremental_helpers.sql

-- Returns the safe incremental cutoff timestamp.
-- Falls back to var('start_date') on first run.
{% macro incremental_cutoff(timestamp_col) %}
    {% if is_incremental() %}
        (select max({{ timestamp_col }}) from {{ this }})
    {% else %}
        '{{ var("start_date") }}'::timestamp
    {% endif %}
{% endmacro %}


-- Generates a fiscal quarter label from a date
{% macro fiscal_quarter_label(date_col) %}
    case
        when extract(month from {{ date_col }}) in (4,5,6)    then 'Q1'
        when extract(month from {{ date_col }}) in (7,8,9)    then 'Q2'
        when extract(month from {{ date_col }}) in (10,11,12) then 'Q3'
        else 'Q4'
    end
{% endmacro %}


-- Masks an email for display (keeps domain, replaces local with ***)
{% macro mask_email(email_col) %}
    case
        when {{ email_col }} is null then null
        else '***@' || split_part({{ email_col }}, '@', 2)
    end
{% endmacro %}


-- Standard audit columns added to every gold mart
{% macro audit_columns() %}
    current_timestamp   as mart_created_at,
    '{{ run_started_at }}' as dbt_run_at,
    '{{ invocation_id }}'  as dbt_invocation_id
{% endmacro %}
