{% macro classify_tactical_score(score) %}
    case 
        when {{ score }} >= 65 then 'High'
        when {{ score }} >= 40 then 'Medium'
        else 'Low'
    end
{% endmacro %}