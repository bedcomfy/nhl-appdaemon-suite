"""Extract play-by-play events."""

from typing import Dict, List, Any
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from nhl_data_extraction.models.game_data import GameEvent

class EventsExtractor:
    """Extracts play-by-play events."""
    
    # Event type mappings
    EVENT_TYPES = {
        502: 'faceoff',
        503: 'hit',
        504: 'giveaway',
        505: 'goal',
        506: 'shot-on-goal',
        507: 'missed-shot',
        508: 'blocked-shot',
        509: 'penalty',
        516: 'stoppage',
        520: 'period-start',
        521: 'period-end',
        524: 'game-end',
        525: 'takeaway',
        535: 'delayed-penalty'
    }
    
    @staticmethod
    def extract(pbp_data: Dict[str, Any], include_all: bool = True) -> List[GameEvent]:
        """
        Extract all play-by-play events.
        
        Args:
            pbp_data: Data from play-by-play endpoint
            include_all: If True, include all events. If False, only major events.
            
        Returns:
            List of GameEvent objects
        """
        events = []
        
        plays = pbp_data.get('plays', [])
        
        for play in plays:
            event_type_code = play.get('typeCode', 0)
            event_type_key = play.get('typeDescKey', '')
            
            # Get event type from our mapping
            event_type = EventsExtractor.EVENT_TYPES.get(event_type_code, event_type_key)
            
            # Skip minor events if include_all is False
            if not include_all and event_type in ['stoppage', 'faceoff']:
                continue
            
            period_desc = play.get('periodDescriptor', {})
            period_num = period_desc.get('number', 0)
            period_type = period_desc.get('periodType', 'REG')
            
            # Format period
            period_display = EventsExtractor._format_period(period_num, period_type)
            
            # Extract player info from details
            details = play.get('details', {})
            player_id = None
            player_name = None
            
            # Try to get primary player involved
            for key in ['scoringPlayerId', 'shootingPlayerId', 'hittingPlayerId', 
                       'committedByPlayerId', 'winningPlayerId', 'playerId']:
                if key in details:
                    player_id = details.get(key)
                    break
            
            event = GameEvent(
                event_id=play.get('eventId', 0),
                period=period_display,
                time=play.get('timeInPeriod', ''),
                time_remaining=play.get('timeRemaining', ''),
                event_type=event_type,
                event_description=event_type_key,
                
                team_id=details.get('eventOwnerTeamId'),
                player_id=player_id,
                player_name=player_name,  # We'd need to look this up from roster
                
                situation_code=play.get('situationCode'),
                
                # Coordinates
                coords={
                    'x': details.get('xCoord'),
                    'y': details.get('yCoord'),
                    'zone': details.get('zoneCode')
                } if 'xCoord' in details else None,
                
                # Store all details for advanced analysis
                details=details
            )
            
            events.append(event)
        
        return events
    
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
    
    @staticmethod
    def get_major_events_only(pbp_data: Dict[str, Any]) -> List[GameEvent]:
        """
        Extract only major events (goals, penalties, shots).
        
        Args:
            pbp_data: Data from play-by-play endpoint
            
        Returns:
            List of GameEvent objects for major events only
        """
        return EventsExtractor.extract(pbp_data, include_all=False)