"""Extract individual player statistics."""

from typing import Dict, Any
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from nhl_data_extraction.models.game_data import PlayerStats

class PlayerStatsExtractor:
    """Extracts individual player statistics."""
    
    @staticmethod
    def extract(boxscore_data: Dict[str, Any]) -> Dict[str, Dict[int, PlayerStats]]:
        """
        Extract player statistics for both teams.
        
        Args:
            boxscore_data: Data from boxscore endpoint
            
        Returns:
            Dictionary: {'home': {player_id: PlayerStats}, 'away': {player_id: PlayerStats}}
        """
        stats = {'home': {}, 'away': {}}
        
        player_by_game = boxscore_data.get('playerByGameStats', {})
        
        for team_key, stats_key in [('homeTeam', 'home'), ('awayTeam', 'away')]:
            team_data = player_by_game.get(team_key, {})
            
            # Process forwards and defense
            for position_group in ['forwards', 'defense']:
                for player in team_data.get(position_group, []):
                    player_id = player.get('playerId', 0)
                    
                    # Extract name
                    name_data = player.get('name', {})
                    name = name_data.get('default', '') if isinstance(name_data, dict) else str(name_data)
                    
                    player_stats = PlayerStats(
                        player_id=player_id,
                        name=name,
                        position=player.get('position', ''),
                        sweater_number=player.get('sweaterNumber', 0),
                        
                        # Scoring
                        goals=player.get('goals', 0),
                        assists=player.get('assists', 0),
                        points=player.get('points', 0),
                        
                        # Other stats
                        plus_minus=player.get('plusMinus', 0),
                        pim=player.get('pim', 0),
                        hits=player.get('hits', 0),
                        sog=player.get('sog', 0),
                        blocked_shots=player.get('blockedShots', 0),
                        giveaways=player.get('giveaways', 0),
                        takeaways=player.get('takeaways', 0),
                        
                        # Advanced
                        power_play_goals=player.get('powerPlayGoals', 0),
                        faceoff_win_pct=player.get('faceoffWinningPctg', 0.0),
                        toi=player.get('toi', '00:00'),
                        shifts=player.get('shifts', 0)
                    )
                    
                    stats[stats_key][player_id] = player_stats
        
        return stats