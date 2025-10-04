"""Extract basic game information."""

from typing import Dict, Any

class GameInfoExtractor:
    """Extracts basic game metadata."""
    
    @staticmethod
    def extract(landing_data: Dict[str, Any], pbp_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract game information.
        
        Args:
            landing_data: Data from landing endpoint
            pbp_data: Data from play-by-play endpoint
            
        Returns:
            Dictionary with game info
        """
        period_desc = landing_data.get('periodDescriptor', {})
        clock = landing_data.get('clock', {})
        
        return {
            'game_id': landing_data.get('id'),
            'season': landing_data.get('season'),
            'game_type': landing_data.get('gameType'),
            'game_date': landing_data.get('gameDate'),
            'game_state': landing_data.get('gameState'),
            'game_schedule_state': landing_data.get('gameScheduleState'),
            
            'venue': landing_data.get('venue', {}).get('default', ''),
            'venue_location': landing_data.get('venueLocation', {}).get('default', ''),
            'start_time_utc': landing_data.get('startTimeUTC'),
            'timezone_offset': landing_data.get('venueUTCOffset', ''),
            'venue_timezone': landing_data.get('venueTimezone', ''),
            
            # Period info
            'current_period': period_desc.get('number', 0),
            'period_type': period_desc.get('periodType', 'REG'),
            'max_regulation_periods': period_desc.get('maxRegulationPeriods', 3),
            
            # Clock
            'time_remaining': clock.get('timeRemaining', '00:00'),
            'seconds_remaining': clock.get('secondsRemaining', 0),
            'clock_running': clock.get('running', False),
            'in_intermission': clock.get('inIntermission', False),
            
            # Game settings
            'shootout_in_use': landing_data.get('shootoutInUse', False),
            'ot_in_use': landing_data.get('otInUse', False),
            'ties_in_use': landing_data.get('tiesInUse', False),
            'max_periods': landing_data.get('maxPeriods', 5),
            'reg_periods': landing_data.get('regPeriods', 3),
        }