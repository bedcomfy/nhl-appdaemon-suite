"""Extract penalty information."""

from typing import Dict, List, Any
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from nhl_data_extraction.models.game_data import Penalty

class PenaltyExtractor:
    """Extracts penalty information."""
    
    @staticmethod
    def extract(landing_data: Dict[str, Any], pbp_data: Dict[str, Any]) -> List[Penalty]:
        """
        Extract all penalties with context.
        
        Args:
            landing_data: Data from landing endpoint
            pbp_data: Data from play-by-play endpoint (for team verification)
            
        Returns:
            List of Penalty objects
        """
        penalties = []
        
        penalty_summary = landing_data.get('summary', {}).get('penalties', [])
        
        for period in penalty_summary:
            period_num = period.get('periodDescriptor', {}).get('number', 0)
            period_type = period.get('periodDescriptor', {}).get('periodType', 'REG')
            
            # Format period display
            period_display = PenaltyExtractor._format_period(period_num, period_type)
            
            for penalty in period.get('penalties', []):
                # Get team from play-by-play for accuracy
                event_id = penalty.get('eventId')
                team_abbrev = PenaltyExtractor._get_penalty_team_from_pbp(event_id, pbp_data)
                
                # Fallback to landing data
                if not team_abbrev:
                    team_data = penalty.get('teamAbbrev', {})
                    team_abbrev = team_data.get('default', '') if isinstance(team_data, dict) else str(team_data)
                
                # Extract player name
                player_name = penalty.get('committedByPlayer', '')
                if isinstance(player_name, dict):
                    player_name = player_name.get('default', '')
                
                # Extract drawn by
                drawn_by = penalty.get('drawnBy', '')
                if isinstance(drawn_by, dict):
                    drawn_by = drawn_by.get('default', '')
                
                # Extract served by (for bench minors)
                served_by = penalty.get('servedBy', '')
                if isinstance(served_by, dict):
                    served_by = served_by.get('default', '')
                
                penalty_obj = Penalty(
                    period=period_display,
                    time=penalty.get('timeInPeriod', ''),
                    team=team_abbrev,
                    player=player_name,
                    penalty_type=penalty.get('descKey', ''),
                    minutes=penalty.get('duration', 0),
                    drawn_by=drawn_by,
                    served_by=served_by,
                    
                    # Additional context
                    coords={
                        'event_id': event_id,
                        'type_code': penalty.get('type', '')
                    }
                )
                
                penalties.append(penalty_obj)
        
        return penalties
    
    @staticmethod
    def _get_penalty_team_from_pbp(event_id: int, pbp_data: Dict[str, Any]) -> str:
        """Get the team that committed a penalty from play-by-play data."""
        if not event_id:
            return ''
        
        plays = pbp_data.get('plays', [])
        
        for play in plays:
            if play.get('eventId') == event_id:
                details = play.get('details', {})
                return details.get('eventOwnerTeamId', '')
        
        return ''
    
    @staticmethod
    def _format_period(period_num: int, period_type: str) -> str:
        """Format period number into readable string."""
        if period_type == 'SO':
            return 'SO'
        elif period_type == 'OT':
            if period_num == 4:
                return 'OT'
            else:
                return f'{period_num - 3}OT'
        else:
            if period_num == 1:
                return '1st'
            elif period_num == 2:
                return '2nd'
            elif period_num == 3:
                return '3rd'
            else:
                return f'{period_num}th'