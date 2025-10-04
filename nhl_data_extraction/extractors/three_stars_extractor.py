"""Extract three stars information."""

from typing import Dict, List, Any
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from nhl_data_extraction.models.game_data import ThreeStar

class ThreeStarsExtractor:
    """Extracts three stars information."""
    
    @staticmethod
    def extract(landing_data: Dict[str, Any]) -> List[ThreeStar]:
        """
        Extract three stars with stats.
        
        Args:
            landing_data: Data from landing endpoint
            
        Returns:
            List of ThreeStar objects
        """
        stars = []
        
        three_stars = landing_data.get('summary', {}).get('threeStars', [])
        
        for star in three_stars:
            # Extract name
            name_data = star.get('name', {})
            name = name_data.get('default', '') if isinstance(name_data, dict) else str(name_data)
            
            # Extract team abbrev
            team_abbrev = star.get('teamAbbrev', '')
            if isinstance(team_abbrev, dict):
                team_abbrev = team_abbrev.get('default', '')
            
            star_obj = ThreeStar(
                star=star.get('star', 0),
                name=name,
                team=team_abbrev,
                position=star.get('position', ''),
                player_id=star.get('playerId', 0),
                sweater_number=star.get('sweaterNo', 0),
                headshot_url=star.get('headshot', ''),
                
                # Stats for the game
                goals=star.get('goals', 0),
                assists=star.get('assists', 0),
                points=star.get('points', 0)
            )
            
            stars.append(star_obj)
        
        return stars