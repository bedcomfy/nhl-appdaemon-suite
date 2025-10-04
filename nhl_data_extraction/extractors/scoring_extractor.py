"""Extract scoring information with video highlights."""

from typing import Dict, List, Any
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from nhl_data_extraction.models.game_data import Goal

class ScoringExtractor:
    """Extracts goal information with video highlights."""
    
    @staticmethod
    def extract(landing_data: Dict[str, Any]) -> List[Goal]:
        """
        Extract all goals with highlights and context.
        
        Args:
            landing_data: Data from landing endpoint
            
        Returns:
            List of Goal objects
        """
        goals = []
        
        scoring = landing_data.get('summary', {}).get('scoring', [])
        
        for period in scoring:
            period_num = period.get('periodDescriptor', {}).get('number', 0)
            period_type = period.get('periodDescriptor', {}).get('periodType', 'REG')
            
            # Format period display - FIXED: Convert to string for dataclass
            period_display = str(ScoringExtractor._format_period(period_num, period_type))
            
            for goal in period.get('goals', []):
                # Extract team abbrev
                team_abbrev = goal.get('teamAbbrev', {})
                if isinstance(team_abbrev, dict):
                    team_abbrev = team_abbrev.get('default', '')
                
                # Extract scorer name
                scorer_name = goal.get('name', {})
                if isinstance(scorer_name, dict):
                    scorer_name = scorer_name.get('default', '')
                
                # Extract assists
                assists = []
                assist_ids = []
                for assist in goal.get('assists', []):
                    assist_name = assist.get('name', {})
                    if isinstance(assist_name, dict):
                        assist_name = assist_name.get('default', '')
                    assists.append(assist_name)
                    assist_ids.append(assist.get('playerId', 0))
                
                goal_obj = Goal(
                    period=period_display,  # Now guaranteed to be a string
                    time=goal.get('timeInPeriod', ''),
                    team=team_abbrev,
                    scorer=scorer_name,
                    scorer_id=goal.get('playerId', 0),
                    assists=assists,
                    assist_ids=assist_ids,
                    strength=goal.get('strength', ''),
                    shot_type=goal.get('shotType', ''),
                    
                    # Video highlights
                    highlight_url=goal.get('highlightClipSharingUrl'),
                    highlight_url_fr=goal.get('highlightClipSharingUrlFr'),
                    discrete_clip=goal.get('discreteClip'),
                    
                    # Score context
                    away_score=goal.get('awayScore', 0),
                    home_score=goal.get('homeScore', 0),
                    
                    # Goal metadata (for advanced users)
                    coords={
                        'situation_code': goal.get('situationCode'),
                        'event_id': goal.get('eventId'),
                        'goal_modifier': goal.get('goalModifier'),
                        'home_team_defending_side': goal.get('homeTeamDefendingSide'),
                        'is_home': goal.get('isHome', False),
                        'goals_to_date': goal.get('goalsToDate', 0),
                        'leading_team': goal.get('leadingTeamAbbrev', {}).get('default', '') if isinstance(goal.get('leadingTeamAbbrev'), dict) else goal.get('leadingTeamAbbrev', ''),
                        'ppt_replay_url': goal.get('pptReplayUrl')
                    }
                )
                
                goals.append(goal_obj)
        
        return goals
    
    @staticmethod
    def _format_period(period_num: int, period_type: str) -> str:
        """
        Format period number into readable string.
        
        Args:
            period_num: Period number from API
            period_type: Period type (REG, OT, SO)
            
        Returns:
            Formatted period string (1st, 2nd, 3rd, OT, 2OT, SO, etc.)
        """
        # Handle shootout
        if period_type == 'SO':
            return 'SO'
        
        # Handle overtime
        if period_type == 'OT':
            if period_num == 4:
                return 'OT'
            else:
                # Multiple overtimes (5th period = 2OT, 6th = 3OT, etc.)
                return f'{period_num - 3}OT'
        
        # Handle regular periods
        if period_num == 1:
            return '1st'
        elif period_num == 2:
            return '2nd'
        elif period_num == 3:
            return '3rd'
        else:
            # Fallback for any unexpected period numbers
            return f'{period_num}th'