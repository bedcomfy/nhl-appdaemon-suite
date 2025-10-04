"""Extract aggregated team statistics."""

from typing import Dict, Any
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from nhl_data_extraction.models.game_data import TeamStats

class TeamStatsExtractor:
    """Extracts aggregated team statistics."""
    
    @staticmethod
    def extract(boxscore_data: Dict[str, Any]) -> TeamStats:
        """
        Extract team statistics by aggregating player stats.
        
        Args:
            boxscore_data: Data from boxscore endpoint
            
        Returns:
            TeamStats object with aggregated statistics
        """
        stats = TeamStats()
        
        # Get shots from top-level team data
        if 'homeTeam' in boxscore_data:
            stats.shots['home'] = boxscore_data['homeTeam'].get('sog')
        if 'awayTeam' in boxscore_data:
            stats.shots['away'] = boxscore_data['awayTeam'].get('sog')
        
        # Aggregate player stats for team totals
        player_by_game = boxscore_data.get('playerByGameStats', {})
        
        for team_key, stats_key in [('homeTeam', 'home'), ('awayTeam', 'away')]:
            if team_key not in player_by_game:
                continue
            
            team_data = player_by_game[team_key]
            
            # Sum stats from all skaters (forwards + defense)
            all_skaters = team_data.get('forwards', []) + team_data.get('defense', [])
            
            total_hits = 0
            total_blocked = 0
            total_giveaways = 0
            total_takeaways = 0
            total_pim = 0
            
            # Faceoff calculations
            total_fow = 0
            total_fol = 0
            
            for player in all_skaters:
                total_hits += player.get('hits', 0)
                total_blocked += player.get('blockedShots', 0)
                total_giveaways += player.get('giveaways', 0)
                total_takeaways += player.get('takeaways', 0)
                total_pim += player.get('pim', 0)
                
                # Faceoff calculation
                fow_pct = player.get('faceoffWinningPctg', 0.0)
                # This is an approximation - we don't have exact faceoff counts
                # so we'll just average the percentages of players who took faceoffs
                if fow_pct > 0:
                    # Player took faceoffs
                    pass  # We'll calculate average below
            
            stats.hits[stats_key] = total_hits
            stats.blocked_shots[stats_key] = total_blocked
            stats.giveaways[stats_key] = total_giveaways
            stats.takeaways[stats_key] = total_takeaways
            stats.pim[stats_key] = total_pim
            
            # Calculate team faceoff percentage (average of players who took faceoffs)
            faceoff_players = [p for p in all_skaters if p.get('faceoffWinningPctg', 0.0) > 0]
            if faceoff_players:
                avg_fow_pct = sum(p.get('faceoffWinningPctg', 0.0) for p in faceoff_players) / len(faceoff_players)
                stats.faceoff_win_pct[stats_key] = round(avg_fow_pct, 3)
        
        return stats