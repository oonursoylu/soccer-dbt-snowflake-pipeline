/* Team-Centric Betting Analysis Macro.
   Determines if a result was 'Expected' or an 'Upset' based on the team's odds.
*/

{% macro is_favorite_upset(team_odds, opponent_odds, draw_odds, team_result) %}
    case 
        -- Team is the Favorite
        when {{ team_odds }} < {{ opponent_odds }} and {{ team_odds }} < {{ draw_odds }} then 
            case when {{ team_result }} = 'W' then 'Expected' else 'Upset' end
        
        -- Team is the Underdog (Opponent is the Favorite)
        when {{ opponent_odds }} < {{ team_odds }} and {{ opponent_odds }} < {{ draw_odds }} then 
            case when {{ team_result }} = 'W' then 'Upset' else 'Expected' end
        
        -- High uncertainty or draw is favored
        else 'High Risk'
    end
{% endmacro %}