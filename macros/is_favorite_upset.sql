{% macro is_favorite_upset(team_odds, opponent_odds, draw_odds, team_result) %}
    case
        -- Team is the clear favorite
        when {{ team_odds }} < {{ opponent_odds }}
         and {{ team_odds }} < {{ draw_odds }}
        then
            case
                when {{ team_result }} = 'W' then 'Expected'
                when {{ team_result }} = 'D' then 'Draw Upset'
                else 'Upset'
            end

        -- Opponent is the clear favorite (team is the underdog)
        when {{ opponent_odds }} < {{ team_odds }}
         and {{ opponent_odds }} < {{ draw_odds }}
        then
            case
                when {{ team_result }} = 'L' then 'Expected'
                when {{ team_result }} = 'D' then 'Draw Upset'
                else 'Upset'
            end

        -- Otherwise: draw favored, or two outcomes tied for lowest odds
        else 'High Risk'
    end
{% endmacro %}