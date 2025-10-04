"""Extract goalie statistics."""

from typing import Dict, Any
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from nhl_data_extraction.models.game_data import GoalieStats

class GoalieStatsExtractor:
    """Extracts goalie statistics."""
    
    @staticmethod
    def extract(boxscore_data: Dict[str, Any]) -> Dict[str, Dict[int, GoalieStats]]:
        """
        Extract goalie statistics for both teams.
        
        Args:
            boxscore_data: Data from boxscore endpoint
            
        Returns:
            Dictionary: {'home': {player_id: GoalieStats}, 'away': {player_id: GoalieStats}}
        """
        stats = {'home': {}, 'away': {}}
        
        player_by_game = boxscore_data.get('playerByGameStats', {})
        
        for team_key, stats_key in [('homeTeam', 'home'), ('awayTeam', 'away')]:
            team_data = player_by_game.get(team_key, {})
            
            for goalie in team_data.get('goalies', []):
                player_id = goalie.get('playerId', 0)
                
                # Extract name
                name_data = goalie.get('name', {})
                name = name_data.get('default', '') if isinstance(name_data, dict) else str(name_data)
                
                goalie_stats = GoalieStats(
                    player_id=player_id,
                    name=name,
                    sweater_number=goalie.get('sweaterNumber', 0),
                    
                    shots_against=goalie.get('shotsAgainst', 0),
                    saves=goalie.get('saves', 0),
                    goals_against=goalie.get('goalsAgainst', 0),
                    save_pct=goalie.get('savePctg', 0.0),
                    
                    even_strength_sa=goalie.get('evenStrengthShotsAgainst', '0/0'),
                    power_play_sa=goalie.get('powerPlayShotsAgainst', '0/0'),
                    shorthanded_sa=goalie.get('shorthandedShotsAgainst', '0/0'),
                    
                    toi=goalie.get('toi', '00:00'),
                    decision=goalie.get('decision')  # W, L, OTL, or None
                )
                
                stats[stats_key][player_id] = goalie_stats
        
        return stats