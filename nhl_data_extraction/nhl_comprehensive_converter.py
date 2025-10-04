"""Comprehensive NHL data converter using modular extractors."""

from typing import Dict, Any, Optional
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from nhl_data_extraction.models.game_data import GameData
from nhl_data_extraction.extractors.game_info_extractor import GameInfoExtractor
from nhl_data_extraction.extractors.team_extractor import TeamExtractor
from nhl_data_extraction.extractors.player_extractor import PlayerExtractor
from nhl_data_extraction.extractors.scoring_extractor import ScoringExtractor
from nhl_data_extraction.extractors.penalty_extractor import PenaltyExtractor
from nhl_data_extraction.extractors.player_stats_extractor import PlayerStatsExtractor
from nhl_data_extraction.extractors.goalie_stats_extractor import GoalieStatsExtractor
from nhl_data_extraction.extractors.team_stats_extractor import TeamStatsExtractor
from nhl_data_extraction.extractors.events_extractor import EventsExtractor
from nhl_data_extraction.extractors.media_extractor import MediaExtractor
from nhl_data_extraction.extractors.three_stars_extractor import ThreeStarsExtractor
from nhl_data_extraction.extractors.on_ice_extractor import OnIceExtractor  # NEW!

class NHLComprehensiveConverter:
    """
    Comprehensive converter that extracts ALL available data from NHL API endpoints.
    
    Uses modular extractors for maintainability and flexibility.
    """
    
    def __init__(self):
        """Initialize the converter."""
        self.game_info_extractor = GameInfoExtractor()
        self.team_extractor = TeamExtractor()
        self.player_extractor = PlayerExtractor()
        self.scoring_extractor = ScoringExtractor()
        self.penalty_extractor = PenaltyExtractor()
        self.player_stats_extractor = PlayerStatsExtractor()
        self.goalie_stats_extractor = GoalieStatsExtractor()
        self.team_stats_extractor = TeamStatsExtractor()
        self.events_extractor = EventsExtractor()
        self.media_extractor = MediaExtractor()
        self.three_stars_extractor = ThreeStarsExtractor()
        self.on_ice_extractor = OnIceExtractor()  # NEW!
    
    def convert(
        self,
        landing_data: Dict[str, Any],
        play_by_play_data: Dict[str, Any],
        boxscore_data: Dict[str, Any],
        include_all_events: bool = False
    ) -> GameData:
        """
        Convert raw NHL API data into comprehensive GameData object.
        
        Args:
            landing_data: Data from landing endpoint
            play_by_play_data: Data from play-by-play endpoint
            boxscore_data: Data from boxscore endpoint
            include_all_events: If True, include all play-by-play events (including stoppages, faceoffs)
            
        Returns:
            GameData object with all extracted information
        """
        # Extract game info
        game_info = self.game_info_extractor.extract(landing_data, play_by_play_data)
        
        # Extract teams
        home_team, away_team = self.team_extractor.extract(landing_data)
        
        # Extract players (rosters with headshots)
        rosters = self.player_extractor.extract(play_by_play_data)
        
        # Extract on-ice players (NEW!)
        on_ice = self.on_ice_extractor.extract_on_ice(landing_data, rosters)
        
        # Extract penalty box (NEW!)
        penalty_box = self.on_ice_extractor.extract_penalty_box(landing_data, rosters)
        
        # Extract scoring
        goals = self.scoring_extractor.extract(landing_data)
        
        # Extract penalties
        penalties = self.penalty_extractor.extract(landing_data, play_by_play_data)
        
        # Extract player stats
        player_stats = self.player_stats_extractor.extract(boxscore_data)
        
        # Extract goalie stats
        goalie_stats = self.goalie_stats_extractor.extract(boxscore_data)
        
        # Extract team stats
        team_stats = self.team_stats_extractor.extract(boxscore_data)
        
        # Extract three stars
        three_stars = self.three_stars_extractor.extract(landing_data)
        
        # Extract events
        events = self.events_extractor.extract(play_by_play_data, include_all=include_all_events)
        
        # Extract media/broadcasts
        broadcasts = self.media_extractor.extract(landing_data)
        
        # Build GameData object
        game_data = GameData(
            # Game info
            game_id=game_info['game_id'],
            season=game_info['season'],
            game_type=game_info['game_type'],
            game_date=game_info['game_date'],
            game_state=game_info['game_state'],
            
            venue=game_info['venue'],
            venue_location=game_info['venue_location'],
            start_time_utc=game_info['start_time_utc'],
            timezone_offset=game_info['timezone_offset'],
            
            # Teams
            home_team=home_team,
            away_team=away_team,
            
            # Period info
            current_period=game_info['current_period'],
            period_type=game_info['period_type'],
            periods_remaining=game_info['max_regulation_periods'] - game_info['current_period'],
            
            # Clock
            time_remaining=game_info['time_remaining'],
            seconds_remaining=game_info['seconds_remaining'],
            clock_running=game_info['clock_running'],
            in_intermission=game_info['in_intermission'],
            
            # Players
            rosters=rosters,
            on_ice=on_ice,  # NEW!
            penalty_box=penalty_box,  # NEW!
            
            # Scoring
            goals=goals,
            
            # Penalties
            penalties=penalties,
            
            # Stats
            player_stats=player_stats,
            goalie_stats=goalie_stats,
            team_stats=team_stats,
            
            # Special
            three_stars=three_stars,
            
            # Events
            events=events,
            
            # Media
            broadcasts=broadcasts,
            
            # Metadata (store additional info)
            metadata={
                'shootout_in_use': game_info['shootout_in_use'],
                'ot_in_use': game_info['ot_in_use'],
                'ties_in_use': game_info['ties_in_use'],
                'max_periods': game_info['max_periods'],
                'reg_periods': game_info['reg_periods'],
                'venue_timezone': game_info['venue_timezone'],
                'game_schedule_state': game_info['game_schedule_state']
            }
        )
        
        return game_data
    
    def convert_to_dict(
        self,
        landing_data: Dict[str, Any],
        play_by_play_data: Dict[str, Any],
        boxscore_data: Dict[str, Any],
        include_all_events: bool = False
    ) -> Dict[str, Any]:
        """
        Convert to dictionary format (for backward compatibility).
        
        Returns a dictionary instead of GameData object.
        """
        game_data = self.convert(landing_data, play_by_play_data, boxscore_data, include_all_events)
        
        # Convert to dict (simplified for now - we can expand this)
        return {
            'game_id': game_data.game_id,
            'game_state': game_data.game_state,
            'season': game_data.season,
            'game_type': game_data.game_type,
            'game_date': game_data.game_date,
            'venue': game_data.venue,
            
            'home_team': game_data.home_team.abbrev,
            'away_team': game_data.away_team.abbrev,
            'home_score': game_data.home_team.score,
            'away_score': game_data.away_team.score,
            
            'goals': [self._goal_to_dict(g) for g in game_data.goals],
            'penalties': [self._penalty_to_dict(p) for p in game_data.penalties],
            'three_stars': [self._three_star_to_dict(s) for s in game_data.three_stars],
            
            'team_stats': {
                'shots': game_data.team_stats.shots,
                'hits': game_data.team_stats.hits,
                'blocked': game_data.team_stats.blocked_shots,
                'giveaways': game_data.team_stats.giveaways,
                'takeaways': game_data.team_stats.takeaways,
                'pim': game_data.team_stats.pim,
                'faceoff_pct': game_data.team_stats.faceoff_win_pct
            } if game_data.team_stats else {},
            
            'broadcasts': [{'network': b.network, 'market': b.market} for b in game_data.broadcasts],
            
            # Rich data
            'player_stats': game_data.player_stats,
            'goalie_stats': game_data.goalie_stats,
            'rosters': game_data.rosters,
            'on_ice': game_data.on_ice,  # NEW!
            'penalty_box': game_data.penalty_box,  # NEW!
            'events': game_data.events if include_all_events else []
        }
    
    def _goal_to_dict(self, goal) -> Dict[str, Any]:
        """Convert Goal object to dict."""
        return {
            'period': goal.period,
            'time': goal.time,
            'team': goal.team,
            'scorer': goal.scorer,
            'scorer_id': goal.scorer_id,
            'assists': goal.assists,
            'assist_ids': goal.assist_ids,
            'strength': goal.strength,
            'shot_type': goal.shot_type,
            'highlight_url': goal.highlight_url,
            'away_score': goal.away_score,
            'home_score': goal.home_score
        }
    
    def _penalty_to_dict(self, penalty) -> Dict[str, Any]:
        """Convert Penalty object to dict."""
        return {
            'period': penalty.period,
            'time': penalty.time,
            'team': penalty.team,
            'player': penalty.player,
            'penalty': penalty.penalty_type,
            'minutes': penalty.minutes,
            'drawn_by': penalty.drawn_by,
            'served_by': penalty.served_by
        }
    
    def _three_star_to_dict(self, star) -> Dict[str, Any]:
        """Convert ThreeStar object to dict."""
        return {
            'star': star.star,
            'name': star.name,
            'team': star.team,
            'position': star.position,
            'player_id': star.player_id,
            'headshot': star.headshot_url,
            'goals': star.goals,
            'assists': star.assists,
            'points': star.points
        }