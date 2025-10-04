"""Extract team information."""

from typing import Dict, Any, Tuple
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from nhl_data_extraction.models.game_data import TeamInfo

class TeamExtractor:
    """Extracts team information."""
    
    @staticmethod
    def extract(landing_data: Dict[str, Any]) -> Tuple[TeamInfo, TeamInfo]:
        """
        Extract home and away team information.
        
        Args:
            landing_data: Data from landing endpoint
            
        Returns:
            Tuple of (home_team, away_team) TeamInfo objects
        """
        home_data = landing_data.get('homeTeam', {})
        away_data = landing_data.get('awayTeam', {})
        
        home_team = TeamInfo(
            id=home_data.get('id', 0),
            abbrev=home_data.get('abbrev', ''),
            name=home_data.get('commonName', {}).get('default', ''),
            place_name=home_data.get('placeName', {}).get('default', ''),
            logo_light=home_data.get('logo', ''),
            logo_dark=home_data.get('darkLogo', ''),
            score=home_data.get('score'),
            sog=home_data.get('sog')
        )
        
        away_team = TeamInfo(
            id=away_data.get('id', 0),
            abbrev=away_data.get('abbrev', ''),
            name=away_data.get('commonName', {}).get('default', ''),
            place_name=away_data.get('placeName', {}).get('default', ''),
            logo_light=away_data.get('logo', ''),
            logo_dark=away_data.get('darkLogo', ''),
            score=away_data.get('score'),
            sog=away_data.get('sog')
        )
        
        return home_team, away_team