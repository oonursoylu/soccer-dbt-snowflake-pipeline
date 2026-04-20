{% macro classify_tactical_score(score) %}
    case
        when {{ score }} is null then null
        when {{ score }} >= 65   then 'High'
        when {{ score }} >= 40   then 'Medium'
        else 'Low'
    end
{% endmacro %}