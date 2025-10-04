"""
NHL Data Service - Comprehensive data fetching and conversion.
Uses the new modular comprehensive converter.

Version: 4.1.0
✅ FIXED: Better game state detection
✅ FIXED: Handles multiple API response structures
✅ FIXED: Only excludes FINAL/OFF games
"""

import asyncio
import httpx
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import sys
import os

# Add apps directory to path for imports
sys.path.insert(0, os.path.dirname(__file__))

from nhl_data_extraction.nhl_comprehensive_converter import NHLComprehensiveConverter
from nhl_data_extraction.models.game_data import GameData

class NHLDataService:
    """
    Service for fetching and converting NHL game data.
    
    Provides both:
    - New comprehensive GameData objects (rich data)
    - Legacy dictionary format (backward compatibility)
    """
    
    def __init__(self):
        """Initialize the data service."""
        self.converter = NHLComprehensiveConverter()
        self.base_url = "https://api-web.nhle.com/v1"
        self.headers = {"User-Agent": "NHL-Dashboard/2.0"}
        self._client: Optional[httpx.AsyncClient] = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        self._client = httpx.AsyncClient(timeout=10.0, headers=self.headers)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self._client:
            await self._client.aclose()
    
    async def _ensure_client(self):
        """Ensure we have an active HTTP client."""
        if not self._client:
            self._client = httpx.AsyncClient(timeout=10.0, headers=self.headers)
    
    async def close(self):
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
    
    # ============================================================================
    # SCHEDULE ENDPOINTS
    # ============================================================================
    
    async def get_schedule(self, date: Optional[str] = None) -> Dict[str, Any]:
        """
        Get schedule for a specific date.
        
        Args:
            date: Date in YYYY-MM-DD format (default: today)
            
        Returns:
            Schedule data from API
        """
        await self._ensure_client()
        
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
        
        url = f"{self.base_url}/schedule/{date}"
        response = await self._client.get(url)
        response.raise_for_status()
        return response.json()
    
    async def get_team_schedule(self, team_abbrev: str, season: Optional[str] = None) -> Dict[str, Any]:
        """
        Get full season schedule for a team.
        
        Args:
            team_abbrev: Team abbreviation (e.g., 'MIN', 'CHI')
            season: Season in format YYYYYYYY (e.g., '20242025'), defaults to current
            
        Returns:
            Team schedule data
        """
        await self._ensure_client()
        
        if season is None:
            # Determine current season (starts in October)
            now = datetime.now()
            if now.month >= 10:
                season = f"{now.year}{now.year + 1}"
            else:
                season = f"{now.year - 1}{now.year}"
        
        url = f"{self.base_url}/club-schedule-season/{team_abbrev}/{season}"
        response = await self._client.get(url)
        response.raise_for_status()
        return response.json()
    
    # ============================================================================
    # GAME DATA ENDPOINTS (The important ones!)
    # ============================================================================
    
    async def fetch_raw_game_data(self, game_id: int) -> Dict[str, Dict[str, Any]]:
        """
        Fetch all 3 raw API endpoints for a game.
        
        Args:
            game_id: NHL game ID
            
        Returns:
            Dictionary with 'landing', 'play_by_play', and 'boxscore' keys
        """
        await self._ensure_client()
        
        # Fetch all 3 endpoints concurrently
        landing_task = self._client.get(f"{self.base_url}/gamecenter/{game_id}/landing")
        pbp_task = self._client.get(f"{self.base_url}/gamecenter/{game_id}/play-by-play")
        boxscore_task = self._client.get(f"{self.base_url}/gamecenter/{game_id}/boxscore")
        
        landing_resp, pbp_resp, boxscore_resp = await asyncio.gather(
            landing_task, pbp_task, boxscore_task
        )
        
        # Raise for any errors
        landing_resp.raise_for_status()
        pbp_resp.raise_for_status()
        boxscore_resp.raise_for_status()
        
        return {
            'landing': landing_resp.json(),
            'play_by_play': pbp_resp.json(),
            'boxscore': boxscore_resp.json()
        }
    
    async def get_game_data(
        self, 
        game_id: int,
        include_all_events: bool = False
    ) -> GameData:
        """
        Get comprehensive game data as GameData object.
        
        Args:
            game_id: NHL game ID
            include_all_events: Include all play-by-play events (faceoffs, stoppages, etc.)
            
        Returns:
            GameData object with all extracted information
        """
        # Fetch raw data
        raw_data = await self.fetch_raw_game_data(game_id)
        
        # Convert to GameData object
        game_data = self.converter.convert(
            raw_data['landing'],
            raw_data['play_by_play'],
            raw_data['boxscore'],
            include_all_events=include_all_events
        )
        
        return game_data
    
    async def get_game_data_dict(
        self,
        game_id: int,
        include_all_events: bool = False
    ) -> Dict[str, Any]:
        """
        Get game data as dictionary (for backward compatibility).
        
        Args:
            game_id: NHL game ID
            include_all_events: Include all play-by-play events
            
        Returns:
            Dictionary with game data
        """
        # Fetch raw data
        raw_data = await self.fetch_raw_game_data(game_id)
        
        # Convert to dictionary
        game_dict = self.converter.convert_to_dict(
            raw_data['landing'],
            raw_data['play_by_play'],
            raw_data['boxscore'],
            include_all_events=include_all_events
        )
        
        return game_dict
    
    # ============================================================================
    # HELPER METHODS
    # ============================================================================
    
    async def get_todays_games(self, team_abbrev: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get today's games, optionally filtered by team.
        
        ✅ FIXED: Improved game state detection
        ✅ FIXED: Only filters out completely finished games
        ✅ FIXED: Includes LIVE, CRIT, FUT, PRE states
        
        Args:
            team_abbrev: Optional team abbreviation to filter by
            
        Returns:
            List of game dictionaries
        """
        today = datetime.now().strftime("%Y-%m-%d")
        
        schedule = await self.get_schedule(date=today)
        
        games = []
        
        # ✅ VALID GAME STATES TO INCLUDE
        # FUT = Future (scheduled)
        # PRE = Pre-game (warmups)
        # LIVE = Live game
        # CRIT = Critical (final minutes)
        # We only exclude FINAL and OFF
        EXCLUDED_STATES = ['FINAL', 'OFF']
        
        # Try gameWeek structure first (most common)
        game_week = schedule.get('gameWeek', [])
        if game_week:
            for game_date in game_week:
                for game in game_date.get('games', []):
                    game_state = game.get('gameState', '')
                    
                    # ✅ ONLY SKIP COMPLETELY FINISHED GAMES
                    if game_state in EXCLUDED_STATES:
                        continue
                    
                    # Filter by team if specified
                    if team_abbrev:
                        away = game.get('awayTeam', {}).get('abbrev', '')
                        home = game.get('homeTeam', {}).get('abbrev', '')
                        if team_abbrev not in [away, home]:
                            continue
                    
                    games.append(game)
        
        # Fallback: try direct 'games' key (alternate API structure)
        if not games and 'games' in schedule:
            for game in schedule.get('games', []):
                game_state = game.get('gameState', '')
                
                if game_state in EXCLUDED_STATES:
                    continue
                
                if team_abbrev:
                    away = game.get('awayTeam', {}).get('abbrev', '')
                    home = game.get('homeTeam', {}).get('abbrev', '')
                    if team_abbrev not in [away, home]:
                        continue
                
                games.append(game)
        
        return games
    
    async def get_live_game_data(self, team_abbrev: str) -> Optional[GameData]:
        """
        Get live game data for a team if they're currently playing.
        
        Args:
            team_abbrev: Team abbreviation
            
        Returns:
            GameData object if game is live, None otherwise
        """
        games = await self.get_todays_games(team_abbrev)
        
        for game in games:
            game_state = game.get('gameState', '')
            
            # ✅ EXPANDED LIVE STATE DETECTION
            if game_state in ['LIVE', 'CRIT', 'PRE']:
                game_id = game.get('id')
                if game_id:
                    return await self.get_game_data(game_id)
        
        return None
    
    async def get_next_game(self, team_abbrev: str) -> Optional[Dict[str, Any]]:
        """
        Get the next scheduled game for a team.
        
        Args:
            team_abbrev: Team abbreviation
            
        Returns:
            Game info dict or None
        """
        schedule = await self.get_team_schedule(team_abbrev)
        now = datetime.now()
        
        for game in schedule.get('games', []):
            game_date_str = game.get('gameDate', '')
            if game_date_str:
                game_date = datetime.fromisoformat(game_date_str.replace('Z', '+00:00'))
                if game_date > now:
                    return game
        
        return None
    
    # ============================================================================
    # STANDINGS & STATS
    # ============================================================================
    
    async def get_standings(self, date: Optional[str] = None) -> Dict[str, Any]:
        """
        Get league standings.
        
        Args:
            date: Date in YYYY-MM-DD format (default: today)
            
        Returns:
            Standings data
        """
        await self._ensure_client()
        
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
        
        url = f"{self.base_url}/standings/{date}"
        response = await self._client.get(url)
        response.raise_for_status()
        return response.json()
    
    async def get_team_stats(self, team_abbrev: str) -> Dict[str, Any]:
        """
        Get current season stats for a team.
        
        Args:
            team_abbrev: Team abbreviation
            
        Returns:
            Team stats
        """
        standings = await self.get_standings()
        
        for standing in standings.get('standings', []):
            if standing.get('teamAbbrev', {}).get('default', '') == team_abbrev:
                return standing
        
        return {}


# ============================================================================
# CONVENIENCE FUNCTIONS (for quick testing/usage)
# ============================================================================

async def get_game(game_id: int) -> GameData:
    """Quick function to get game data."""
    async with NHLDataService() as service:
        return await service.get_game_data(game_id)

async def get_live_game(team_abbrev: str) -> Optional[GameData]:
    """Quick function to get live game for a team."""
    async with NHLDataService() as service:
        return await service.get_live_game_data(team_abbrev)


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

async def example_usage():
    """Example of how to use the service."""
    async with NHLDataService() as service:
        # Get today's schedule
        schedule = await service.get_schedule()
        print(f"Games today: {len(schedule.get('gameWeek', []))}")
        
        # Get a specific game with full data
        game_data = await service.get_game_data(2025010086)
        print(f"\nGame: {game_data.away_team.abbrev} @ {game_data.home_team.abbrev}")
        print(f"Score: {game_data.away_team.score} - {game_data.home_team.score}")
        print(f"State: {game_data.game_state}")
        
        # Access rich data
        print(f"\n⭐ Three Stars:")
        for star in game_data.three_stars:
            print(f"  {star.star}. {star.name} - {star.goals}G {star.assists}A")
        
        print(f"\n⚽ Goals ({len(game_data.goals)}):")
        for goal in game_data.goals:
            print(f"  {goal.period} @ {goal.time} - {goal.scorer}")
            print(f"    🎥 {goal.highlight_url}")
        
        # Get live game for a team
        live_game = await service.get_live_game_data('MIN')
        if live_game:
            print(f"\n🔴 LIVE: {live_game.away_team.abbrev} @ {live_game.home_team.abbrev}")
            print(f"Period {live_game.current_period} - {live_game.time_remaining}")
        else:
            print("\nNo live game for MIN")


if __name__ == "__main__":
    asyncio.run(example_usage())