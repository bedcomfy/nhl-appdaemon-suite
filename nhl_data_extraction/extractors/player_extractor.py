"""Extract player roster information."""

from typing import Dict, List, Any
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from nhl_data_extraction.models.game_data import PlayerInfo

class PlayerExtractor:
    """Extracts player roster information."""
    
    @staticmethod
    def extract(pbp_data: Dict[str, Any]) -> Dict[int, List[PlayerInfo]]:
        """
        Extract player rosters with headshots.
        
        Args:
            pbp_data: Data from play-by-play endpoint
            
        Returns:
            Dictionary mapping team_id to list of PlayerInfo objects
        """
        rosters: Dict[int, List[PlayerInfo]] = {}
        
        roster_spots = pbp_data.get('rosterSpots', [])
        
        for player in roster_spots:
            team_id = player.get('teamId')
            
            if team_id not in rosters:
                rosters[team_id] = []
            
            # Handle multi-language names
            first_name_data = player.get('firstName', {})
            last_name_data = player.get('lastName', {})
            
            first_name = first_name_data.get('default', '') if isinstance(first_name_data, dict) else str(first_name_data)
            last_name = last_name_data.get('default', '') if isinstance(last_name_data, dict) else str(last_name_data)
            
            player_info = PlayerInfo(
                player_id=player.get('playerId', 0),
                team_id=team_id,
                first_name=first_name,
                last_name=last_name,
                full_name=f"{first_name} {last_name}",
                sweater_number=player.get('sweaterNumber', 0),
                position=player.get('positionCode', ''),
                headshot_url=player.get('headshot', '')
            )
            
            rosters[team_id].append(player_info)
        
        return rosters