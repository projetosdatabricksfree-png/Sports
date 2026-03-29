-- =============================================================================
-- Macro: rolling_avg
-- Calcula média móvel de N linhas anteriores (inclusive a atual)
-- Uso: {{ rolling_avg('column', 'partition_col', 'order_col', 5) }}
-- =============================================================================
{% macro rolling_avg(column, partition_by, order_by, window=5) %}
    avg({{ column }}) over (
        partition by {{ partition_by }}
        order by {{ order_by }}
        rows between {{ window - 1 }} preceding and current row
    )
{% endmacro %}
