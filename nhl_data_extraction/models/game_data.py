"""
Data models for NHL game data.
Comprehensive models covering all aspects of an NHL game.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from datetime import datetime


@dataclass
class Team:
    """Team information."""
    id: int
    name: str
    abbrev: str
    logo_light: Optional[str] = None
    logo_dark: Optional[str] = None
    score: int = 0
    sog: int = 0  # Shots on goal


@dataclass
class Player:
    """Player information."""
    player_id: int
    full_name: str
    first_name: str
    last_name: str
    sweater_number: int
    position: str
    headshot_url: Optional[str] = None


@dataclass
class Goal:
    """Goal information."""
    period: str  # Already formatted: "1st", "2nd", "OT", "SO"
    time: str
    team: str
    scorer: str
    scorer_id: int
    assists: List[str]
    assist_ids: List[int]
    strength: str
    shot_type: Optional[str] = None
    goal_modifier: Optional[str] = None
    away_score: int = 0
    home_score: int = 0
    highlight_url: Optional[str] = None
    highlight_url_fr: Optional[str] = None


@dataclass
class Penalty:
    """Penalty information."""
    period: str  # Already formatted: "1st", "2nd", "OT"
    time: str
    team: str
    player: str
    player_id: Optional[int] = None
    penalty_type: str = ""
    minutes: int = 0
    drawn_by: Optional[str] = None
    drawn_by_id: Optional[int] = None
    served_by: Optional[str] = None
    served_by_id: Optional[int] = None


@dataclass
class PlayerStats:
    """Individual player statistics."""
    player_id: int
    name: str
    position: str
    team_id: int
    
    # Skating stats
    goals: int = 0
    assists: int = 0
    points: int = 0
    shots: int = 0
    hits: int = 0
    blocked_shots: int = 0
    pim: int = 0  # Penalty minutes
    plus_minus: int = 0
    powerplay_goals: int = 0
    shorthanded_goals: int = 0
    game_winning_goals: int = 0
    
    # Faceoffs
    faceoff_wins: int = 0
    faceoff_losses: int = 0
    faceoff_pct: float = 0.0
    
    # Time
    toi: str = "0:00"  # Time on ice
    powerplay_toi: str = "0:00"
    shorthanded_toi: str = "0:00"


@dataclass
class GoalieStats:
    """Goalie statistics."""
    player_id: int
    name: str
    sweater_number: int
    team_id: int
    
    saves: int = 0
    shots_against: int = 0
    goals_against: int = 0
    save_pct: float = 0.0
    toi: str = "0:00"
    decision: Optional[str] = None  # "W", "L", "OTL", etc.
    even_strength_saves: int = 0
    powerplay_saves: int = 0
    shorthanded_saves: int = 0
    even_strength_shots: int = 0
    powerplay_shots: int = 0
    shorthanded_shots: int = 0


@dataclass
class TeamStats:
    """Team statistics."""
    shots: Dict[str, int] = field(default_factory=lambda: {'home': 0, 'away': 0})
    hits: Dict[str, int] = field(default_factory=lambda: {'home': 0, 'away': 0})
    blocked_shots: Dict[str, int] = field(default_factory=lambda: {'home': 0, 'away': 0})
    giveaways: Dict[str, int] = field(default_factory=lambda: {'home': 0, 'away': 0})
    takeaways: Dict[str, int] = field(default_factory=lambda: {'home': 0, 'away': 0})
    pim: Dict[str, int] = field(default_factory=lambda: {'home': 0, 'away': 0})
    faceoff_win_pct: Dict[str, float] = field(default_factory=lambda: {'home': 0.0, 'away': 0.0})
    powerplay: Dict[str, str] = field(default_factory=lambda: {'home': '0/0', 'away': '0/0'})


@dataclass
class ThreeStar:
    """Three stars of the game."""
    star: int  # 1, 2, or 3
    name: str
    player_id: int
    team: str
    position: str
    sweater_number: int
    headshot_url: Optional[str] = None
    goals: int = 0
    assists: int = 0
    points: int = 0


@dataclass
class GameEvent:
    """Play-by-play event."""
    event_id: int
    event_type: str
    period: str  # Already formatted
    time: str
    time_in_period: str
    team_id: Optional[int] = None
    player_id: Optional[int] = None
    player_name: Optional[str] = None
    event_description: str = ""
    x_coord: Optional[float] = None
    y_coord: Optional[float] = None


@dataclass
class Broadcast:
    """Broadcast information."""
    network: str
    market: str  # "N" for national, "H" for home, "A" for away
    country_code: str = "US"


@dataclass
class GameData:
    """
    Complete game data model.
    Contains all information about an NHL game.
    """
    
    # ========================================================================
    # GAME IDENTIFICATION
    # ========================================================================
    game_id: int
    season: str
    game_type: str  # "PR" (preseason), "R" (regular), "P" (playoffs)
    game_date: str
    game_state: str  # "FUT", "PRE", "LIVE", "CRIT", "FINAL", "OFF"
    
    # ========================================================================
    # VENUE & TIMING
    # ========================================================================
    venue: str
    venue_location: Optional[str] = None
    start_time_utc: Optional[str] = None
    timezone_offset: Optional[str] = None
    
    # ========================================================================
    # TEAMS
    # ========================================================================
    home_team: Team = None
    away_team: Team = None
    
    # ========================================================================
    # PERIOD INFORMATION
    # ========================================================================
    current_period: int = 0
    period_type: str = "REG"  # "REG", "OT", "SO"
    periods_remaining: int = 3
    
    # ========================================================================
    # CLOCK
    # ========================================================================
    time_remaining: Optional[str] = None
    seconds_remaining: int = 0
    clock_running: bool = False
    in_intermission: bool = False
    
    # ========================================================================
    # PLAYERS
    # ========================================================================
    rosters: Dict[int, List[Player]] = field(default_factory=dict)
    on_ice: Dict[str, List[Dict[str, Any]]] = field(default_factory=lambda: {'home': [], 'away': []})
    penalty_box: Dict[str, List[Dict[str, Any]]] = field(default_factory=lambda: {'home': [], 'away': []})
    
    # ========================================================================
    # SCORING
    # ========================================================================
    goals: List[Goal] = field(default_factory=list)
    
    # ========================================================================
    # PENALTIES
    # ========================================================================
    penalties: List[Penalty] = field(default_factory=list)
    
    # ========================================================================
    # STATISTICS
    # ========================================================================
    player_stats: Dict[str, Dict[int, PlayerStats]] = field(default_factory=dict)
    goalie_stats: Dict[str, Dict[int, GoalieStats]] = field(default_factory=dict)
    team_stats: Optional[TeamStats] = None
    
    # ========================================================================
    # SPECIAL RECOGNITION
    # ========================================================================
    three_stars: List[ThreeStar] = field(default_factory=list)
    
    # ========================================================================
    # PLAY-BY-PLAY
    # ========================================================================
    events: List[GameEvent] = field(default_factory=list)
    
    # ========================================================================
    # MEDIA
    # ========================================================================
    broadcasts: List[Broadcast] = field(default_factory=list)
    
    # ========================================================================
    # METADATA
    # ========================================================================
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Initialize optional fields with defaults if None."""
        if self.on_ice is None:
            self.on_ice = {'home': [], 'away': []}
        if self.penalty_box is None:
            self.penalty_box = {'home': [], 'away': []}
        if self.player_stats is None:
            self.player_stats = {}
        if self.goalie_stats is None:
            self.goalie_stats = {}
    
    # ========================================================================
    # UTILITY METHODS
    # ========================================================================
    
    def is_live(self) -> bool:
        """Check if game is currently live."""
        return self.game_state in ["LIVE", "CRIT"]
    
    def is_final(self) -> bool:
        """Check if game is final."""
        return self.game_state in ["FINAL", "OFF"]
    
    def is_scheduled(self) -> bool:
        """Check if game is scheduled but not started."""
        return self.game_state in ["FUT", "PRE"]
    
    def get_score_differential(self) -> int:
        """Get score differential (home - away)."""
        if self.home_team and self.away_team:
            return self.home_team.score - self.away_team.score
        return 0
    
    def get_total_goals(self) -> int:
        """Get total goals scored in game."""
        return len(self.goals)
    
    def get_period_goals(self, period: str) -> List[Goal]:
        """Get goals for a specific period."""
        return [g for g in self.goals if g.period == period]
    
    def get_team_goals(self, team_abbrev: str) -> List[Goal]:
        """Get goals for a specific team."""
        return [g for g in self.goals if g.team == team_abbrev]
    
    def get_on_ice_count(self, side: str = 'home') -> int:
        """Get number of players on ice for home or away."""
        return len(self.on_ice.get(side, []))
    
    def get_penalty_box_count(self, side: str = 'home') -> int:
        """Get number of players in penalty box for home or away."""
        return len(self.penalty_box.get(side, []))
    
    def get_strength_situation(self) -> str:
        """
        Get current strength situation.
        Returns: "Even", "Power Play", "Shorthanded", etc.
        """
        home_count = self.get_on_ice_count('home')
        away_count = self.get_on_ice_count('away')
        
        if home_count == away_count:
            return "Even Strength"
        elif home_count > away_count:
            return "Home Power Play"
        elif away_count > home_count:
            return "Away Power Play"
        
        return "Unknown"