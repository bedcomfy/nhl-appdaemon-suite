# /config/appdaemon/apps/nhl_dashboard_manager.py
import appdaemon.plugins.hass.hassapi as hass
import datetime
import asyncio
import sys
import os
from typing import Optional, Dict, Any, Tuple, List

# Add apps directory to path
sys.path.insert(0, os.path.dirname(__file__))

# Import the new data service
from nhl_data_service import NHLDataService
from nhl_data_extraction.models.game_data import GameData

# Import shared constants
from nhl_const import NHL_TEAM_DETAILS_MAP


class NhlDashboardManager(hass.Hass):
    APP_VERSION = "4.1.0"  # Added on-ice and penalty box support

    def initialize(self):
        self.log_level = self.args.get("log_level", "INFO").upper()
        self.log_message(f"NHL Dashboard Manager App Initializing (v{self.APP_VERSION})...", level="INFO")
        
        # Configuration
        self.team_preset_select = self.args.get("team_notification_preset_select")
        self.ha_sensor_entity_id = self.args.get("ha_sensor_entity_id")
        self.refresh_interval_live = self.args.get("refresh_interval_live", 5)
        self.refresh_interval_upcoming = self.args.get("refresh_interval_upcoming", 60)
        self.refresh_interval_off = self.args.get("refresh_interval_off", 300)
        self.refresh_interval_error = self.args.get("refresh_interval_error", 30)
        
        self.timer_handle = None
        self.DEFAULT_NHL_LOGO = "https://assets.nhle.com/logos/nhl/svg/NHL_light.svg"
        
        # Initialize the data service
        self.data_service = None

        if not self.team_preset_select or not self.ha_sensor_entity_id:
            self.log_message("CRITICAL: Missing 'team_notification_preset_select' or 'ha_sensor_entity_id' in config.", level="ERROR")
            return

        self.listen_state(self.team_selection_changed_callback, self.team_preset_select)
        self.create_task(self._initial_setup())
        self.log_message("NHL Dashboard Manager App Initialized.", level="INFO")

    async def _initial_setup(self):
        """Initial setup and data fetch."""
        # Create data service instance
        self.data_service = NHLDataService()
        
        # Get initial team selection
        initial_selected_team_name = await self.get_state(self.team_preset_select)
        if initial_selected_team_name and isinstance(initial_selected_team_name, str) and initial_selected_team_name.lower() != "none":
            await self.fetch_and_update_data_for_team(initial_selected_team_name)
        else:
            self._update_sensor_no_team_selected()
            self._schedule_next_refresh(self.refresh_interval_off)

    def _get_team_abbrev_from_preset(self, preset_name: str) -> Optional[str]:
        if not preset_name or preset_name.lower() == "none":
            return None
        for abbrev, details in NHL_TEAM_DETAILS_MAP.items():
            if details["full_name"].lower() == preset_name.lower():
                return abbrev
        return None

    def log_message(self, message: str, level: str = "INFO") -> None:
        self.log(f"[DASHBOARD_MGR_V{self.APP_VERSION}] {message}", level=level.upper())

    def _get_value_or_default(self, data_item: Any, default_value: Any = None) -> Any:
        if isinstance(data_item, dict):
            return data_item.get("default", default_value)
        elif isinstance(data_item, str):
            return data_item
        return default_value

    def _format_ordinal(self, period_num: Optional[int], period_type: Optional[str] = None) -> str:
        """Format period number to ordinal string (1st, 2nd, 3rd, OT, etc.)."""
        if period_num is None:
            return ""
        
        # Convert to int if it's a string
        if isinstance(period_num, str):
            try:
                period_num = int(period_num)
            except (ValueError, TypeError):
                return str(period_num)  # Return as-is if can't convert
        
        ptype_norm = (period_type or "").strip().upper()
        
        if ptype_norm == "SO":
            return "SO"
        if period_num == 1:
            return "1st"
        if period_num == 2:
            return "2nd"
        if period_num == 3:
            return "3rd"
        if period_num == 4 and ptype_norm == "OT":
            return "OT"
        if period_num > 4 and ptype_norm == "OT":
            return f"{period_num - 3}OT"
        return f"{period_num}th"

    # ========================================================================
    # Transform GameData to sensor attributes
    # ========================================================================
    
    def _transform_game_data_to_attributes(
        self, 
        game_data: GameData,
        selected_team_abbrev: str
    ) -> Tuple[str, Dict[str, Any]]:
        """Transform comprehensive GameData object into sensor attributes."""
        
        attributes = {
            "selected_team_abbr": selected_team_abbrev,
            "selected_team_name": NHL_TEAM_DETAILS_MAP.get(selected_team_abbrev, {}).get("full_name", selected_team_abbrev),
            
            # Basic game info
            "game_id": game_data.game_id,
            "game_state_api": game_data.game_state,
            "game_start_time_utc": game_data.start_time_utc,
            "venue": game_data.venue,
            "game_url": f"https://www.nhl.com/gamecenter/{game_data.game_id}",
            
            # Teams
            "home_name": game_data.home_team.name,
            "home_abbr": game_data.home_team.abbrev,
            "home_logo": game_data.home_team.logo_light or self.DEFAULT_NHL_LOGO,
            "home_logo_dark": game_data.home_team.logo_dark,
            "home_score": game_data.home_team.score,
            "home_sog": game_data.home_team.sog,
            
            "away_name": game_data.away_team.name,
            "away_abbr": game_data.away_team.abbrev,
            "away_logo": game_data.away_team.logo_light or self.DEFAULT_NHL_LOGO,
            "away_logo_dark": game_data.away_team.logo_dark,
            "away_score": game_data.away_team.score,
            "away_sog": game_data.away_team.sog,
            
            # Game clock
            "period": game_data.current_period,
            "period_ord": self._format_ordinal(game_data.current_period, game_data.period_type),
            "period_type": game_data.period_type,
            "time_remaining": game_data.time_remaining or "--:--",
            "in_intermission": game_data.in_intermission,
            "clock_running": game_data.clock_running,
            
            # Goals
            "goals": self._format_goals(game_data.goals),
            "scoring_detailed": self._format_scoring_by_period(game_data.goals),
            
            # Penalties
            "penalties_detailed": self._format_penalties_by_period(game_data.penalties),
            
            # Three stars (with headshots!)
            "three_stars": self._format_three_stars(game_data.three_stars),
            
            # Team statistics
            "shots": {
                "home": game_data.team_stats.shots.get('home') if game_data.team_stats else None,
                "away": game_data.team_stats.shots.get('away') if game_data.team_stats else None
            },
            "hits": {
                "home": game_data.team_stats.hits.get('home', 0) if game_data.team_stats else 0,
                "away": game_data.team_stats.hits.get('away', 0) if game_data.team_stats else 0
            },
            "blocked": {
                "home": game_data.team_stats.blocked_shots.get('home', 0) if game_data.team_stats else 0,
                "away": game_data.team_stats.blocked_shots.get('away', 0) if game_data.team_stats else 0
            },
            "pim": {
                "home": game_data.team_stats.pim.get('home', 0) if game_data.team_stats else 0,
                "away": game_data.team_stats.pim.get('away', 0) if game_data.team_stats else 0
            },
            "faceoffs": {
                "home": game_data.team_stats.faceoff_win_pct.get('home') if game_data.team_stats else None,
                "away": game_data.team_stats.faceoff_win_pct.get('away') if game_data.team_stats else None
            },
            
            # Goalies
            "goalies": self._format_goalies(game_data),
            
            # Rosters (with headshots!)
            "home_roster": self._format_roster(game_data.rosters.get(game_data.home_team.id, [])),
            "away_roster": self._format_roster(game_data.rosters.get(game_data.away_team.id, [])),
            
            # On-ice players (NEW!)
            "on_ice": game_data.on_ice,
            
            # Penalty box (NEW!)
            "penalty_box": game_data.penalty_box,
            
            # Special teams state (NEW!)
            "special_teams_state": game_data.get_strength_situation(),
            
            # Broadcasts
            "national_broadcasts": [b.network for b in game_data.broadcasts if b.market == "N"],
            "broadcasters_by_market": [{"network": b.network, "market": b.market, "country": b.country_code} for b in game_data.broadcasts],
            
            # Last play/event tracking
            "last_play": "N/A",
            "last_event": {},
            "last_event_id": None,
            
            # Play-by-play feed
            "plays_feed": self._format_plays_feed(game_data.events[:120] if game_data.events else []),
            
            # Game clock detail
            "game_clock": {
                "running": game_data.clock_running,
                "in_intermission": game_data.in_intermission,
                "period": game_data.current_period,
                "period_ord": self._format_ordinal(game_data.current_period, game_data.period_type),
                "period_type": game_data.period_type,
                "time_remaining": game_data.time_remaining or "--:--",
            },
            
            # Metadata
            "last_api_update_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "attribution": "Data provided by NHL API via AppDaemon",
            "error_message": "",
        }
        
        # Set last play from latest event
        if game_data.events:
            latest = game_data.events[0]  # Events are reversed
            attributes["last_play"] = self._format_event_description(latest)
            attributes["last_event_id"] = latest.event_id
        
        # Determine sensor state
        if game_data.game_state in ["LIVE", "CRIT"]:
            state = "LIVE"
        elif game_data.game_state in ["FINAL", "OFF"]:
            state = "FINAL"
            attributes["time_remaining"] = "FINAL"
        elif game_data.game_state == "PPD":
            state = "POSTPONED"
        elif game_data.start_time_utc:
            try:
                game_time_utc = datetime.datetime.fromisoformat(str(game_data.start_time_utc).replace("Z", "+00:00"))
                state = "UPCOMING" if game_time_utc > datetime.datetime.now(datetime.timezone.utc) else "SCHEDULED_PAST"
            except ValueError:
                state = "UNKNOWN"
        else:
            state = "UNKNOWN"
        
        return state, attributes
    
    def _format_goals(self, goals: List) -> List[Dict[str, Any]]:
        """Format goals for sensor attributes."""
        formatted = []
        for goal in goals:
            formatted.append({
                "period_ord": goal.period,  # Already formatted: "1st", "2nd", "SO", etc.
                "time": goal.time,
                "team_abbrev": goal.team,
                "scorer_name": goal.scorer,
                "assists": [{"name": a} for a in goal.assists],
                "strength": goal.strength,
                "shot_type": goal.shot_type,
                "highlight_url": goal.highlight_url,
                "highlight_url_fr": goal.highlight_url_fr,
            })
        return formatted
    
    def _format_scoring_by_period(self, goals: List) -> Dict[str, List]:
        """Group goals by period."""
        by_period = {}
        for goal in goals:
            period_key = goal.period if goal.period else "unknown"
            
            if period_key not in by_period:
                by_period[period_key] = []
            by_period[period_key].append({
                "team": goal.team,
                "scorer": goal.scorer,
                "assists": goal.assists,
                "time": goal.time,
                "strength": goal.strength,
                "shot_type": goal.shot_type,
                "highlight_url": goal.highlight_url,
            })
        return by_period
    
    def _format_penalties_by_period(self, penalties: List) -> Dict[str, List]:
        """Group penalties by period."""
        by_period = {}
        for penalty in penalties:
            period_key = penalty.period if penalty.period else "unknown"
            
            if period_key not in by_period:
                by_period[period_key] = []
            by_period[period_key].append({
                "team": penalty.team,
                "team_abbr": penalty.team,
                "time": penalty.time,
                "who": penalty.player,
                "name": penalty.penalty_type,
                "minutes": penalty.minutes,
                "drawn_by": penalty.drawn_by,
            })
        return by_period
    
    def _format_three_stars(self, stars: List) -> List[Dict[str, Any]]:
        """Format three stars with headshots."""
        formatted = []
        for star in stars:
            formatted.append({
                "fullName": star.name,
                "sweaterNumber": str(star.sweater_number),
                "position": star.position,
                "teamAbbr": star.team,
                "starIndex": star.star,
                "playerId": star.player_id,
                "headshot_url": star.headshot_url,
                "goals": star.goals,
                "assists": star.assists,
                "points": star.points,
            })
        return formatted
    
    def _format_goalies(self, game_data: GameData) -> List[Dict[str, Any]]:
        """Format goalie statistics from nested dict structure."""
        formatted = []
        
        if not game_data.goalie_stats:
            return formatted
        
        home_abbr = game_data.home_team.abbrev
        away_abbr = game_data.away_team.abbrev
        
        # Process home goalies
        home_goalies = game_data.goalie_stats.get('home', {})
        for player_id, goalie in home_goalies.items():
            formatted.append({
                "name": goalie.name,
                "team": home_abbr,
                "sweater": goalie.sweater_number,
                "saves": goalie.saves,
                "shots_against": goalie.shots_against,
                "save_pct": round(goalie.save_pct, 3) if goalie.save_pct else 0.0,
                "goals_against": goalie.goals_against,
                "toi": goalie.toi,
                "decision": goalie.decision or "",
                "headshot_url": f"https://assets.nhle.com/mugs/nhl/20242025/{home_abbr}/{player_id}.png",
            })
        
        # Process away goalies
        away_goalies = game_data.goalie_stats.get('away', {})
        for player_id, goalie in away_goalies.items():
            formatted.append({
                "name": goalie.name,
                "team": away_abbr,
                "sweater": goalie.sweater_number,
                "saves": goalie.saves,
                "shots_against": goalie.shots_against,
                "save_pct": round(goalie.save_pct, 3) if goalie.save_pct else 0.0,
                "goals_against": goalie.goals_against,
                "toi": goalie.toi,
                "decision": goalie.decision or "",
                "headshot_url": f"https://assets.nhle.com/mugs/nhl/20242025/{away_abbr}/{player_id}.png",
            })
        
        return formatted
    
    def _format_roster(self, roster: List) -> List[Dict[str, Any]]:
        """Format team roster with headshots."""
        formatted = []
        for player in roster:
            formatted.append({
                "id": player.player_id,
                "name": player.full_name,
                "sweater": player.sweater_number,
                "position": player.position,
                "headshot_url": player.headshot_url,
            })
        return formatted
    
    def _format_plays_feed(self, events: List) -> List[Dict[str, Any]]:
        """Format play-by-play events."""
        feed = []
        for event in events:
            feed.append({
                "id": event.event_id,
                "type": event.event_type,
                "period": event.period,
                "periodOrd": event.period,
                "time": event.time,
                "team": event.team_id,
                "desc": event.event_description,
            })
        return feed
    
    def _format_event_description(self, event) -> str:
        """Create human-readable event description."""
        return event.event_description

    # ========================================================================
    # Fetch and update logic
    # ========================================================================

    async def team_selection_changed_callback(self, entity: str, attribute: str, old_state: str, new_state: str, kwargs: Dict) -> None:
        if self.timer_handle:
            self.cancel_timer(self.timer_handle)
        if new_state and isinstance(new_state, str) and new_state.lower() != "none":
            await self.fetch_and_update_data_for_team(new_state)
        else:
            self._update_sensor_no_team_selected()
            self._schedule_next_refresh(self.refresh_interval_off)

    async def fetch_and_update_data_for_team(self, team_name_preset: str) -> None:
        import time
        fetch_start = time.time()

        team_abbrev = self._get_team_abbrev_from_preset(team_name_preset)
        if not team_abbrev:
            self._update_sensor_no_team_selected()
            self._schedule_next_refresh(self.refresh_interval_error)
            return

        try:
            games = await self.data_service.get_todays_games(team_abbrev)
            
            if not games:
                self._update_sensor_no_game_scheduled(team_abbrev)
                self._schedule_next_refresh(self.refresh_interval_off)
                return
            
            game = games[0]
            game_id = game.get("id")
            
            if not game_id:
                self._update_sensor_no_game_scheduled(team_abbrev)
                self._schedule_next_refresh(self.refresh_interval_off)
                return
            
            self.log_message(f"Fetching comprehensive data for game {game_id}", level="INFO")
            game_data = await self.data_service.get_game_data(game_id, include_all_events=False)
            
            # Transform to sensor attributes
            state, attributes = self._transform_game_data_to_attributes(game_data, team_abbrev)
            
            # Update sensor
            self._update_sensor(state, attributes)
            
            # Determine refresh interval
            next_refresh_interval = self._determine_refresh_interval(state, attributes)
            
        except Exception as e:
            self.log_message(f"Error fetching game data: {e}", level="ERROR")
            import traceback
            self.log_message(f"Traceback: {traceback.format_exc()}", level="ERROR")
            self._update_sensor_no_game_scheduled(team_abbrev)
            next_refresh_interval = self.refresh_interval_error

        fetch_time = time.time() - fetch_start
        self.log_message(f"Full fetch cycle took {fetch_time:.2f}s. Scheduling next in {next_refresh_interval}s.", level="DEBUG")
        self._schedule_next_refresh(next_refresh_interval)

    def _determine_refresh_interval(self, state: str, attributes: Dict[str, Any]) -> int:
        """Determine how long until next refresh based on game state."""
        if state == "LIVE":
            period = attributes.get("period", 0)
            time_rem_str = attributes.get("time_remaining", "00:00")
            
            try:
                mins_rem = int(time_rem_str.split(":")[0]) if ":" in time_rem_str and time_rem_str not in ("FINAL", "INTERMISSION") else 99
                if period >= 3 and mins_rem <= 2:
                    self.log_message("High-attention (P3<=2m). Polling every 3s.", level="INFO")
                    return 3
                else:
                    return self.refresh_interval_live
            except (ValueError, AttributeError):
                return self.refresh_interval_live
        elif state in ["UPCOMING", "SCHEDULED_PAST"]:
            return self.refresh_interval_upcoming
        else:
            return self.refresh_interval_off

    # ========================================================================
    # Sensor update helpers
    # ========================================================================

    def _update_sensor(self, state: str, attributes: Dict[str, Any]) -> None:
        try:
            self.set_state(self.ha_sensor_entity_id, state=state, attributes=attributes)
        except Exception as e:
            self.log_message(f"Error setting sensor state: {e}", level="ERROR")
        self.log_message(f"Sensor '{self.ha_sensor_entity_id}' updated. State: {state}.", level="INFO")

    def _update_sensor_no_game_scheduled(self, team_abbrev: str) -> None:
        attributes = {
            "selected_team_abbr": team_abbrev,
            "selected_team_name": NHL_TEAM_DETAILS_MAP.get(team_abbrev, {}).get("full_name", team_abbrev),
            "error_message": "No game found for selected team.",
            "game_state_api": "NO_GAME_SCHEDULED",
            "last_api_update_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "attribution": "Data provided by NHL API via AppDaemon",
        }
        self._update_sensor("No Game Scheduled", attributes)

    def _update_sensor_no_team_selected(self) -> None:
        attributes = {
            "error_message": "No team selected in preset.",
            "game_state_api": "NO_TEAM_SELECTED",
            "last_api_update_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "attribution": "Data provided by NHL API via AppDaemon",
        }
        self._update_sensor("No Team Selected", attributes)

    def _schedule_next_refresh(self, interval_seconds: int) -> None:
        if self.timer_handle:
            try:
                if self.timer_running(self.timer_handle):
                    self.cancel_timer(self.timer_handle)
            except Exception:
                pass
        safe_interval = max(interval_seconds, 3)
        self.timer_handle = self.run_in(self.scheduled_refresh_callback_wrapper, safe_interval)
        self.log_message(f"Scheduling next refresh in {safe_interval} seconds.", level="DEBUG")

    def scheduled_refresh_callback_wrapper(self, kwargs: Dict) -> None:
        self.timer_handle = None
        self.create_task(self.scheduled_refresh_callback(kwargs))

    async def scheduled_refresh_callback(self, kwargs: Dict) -> None:
        current_selected_team_name = await self.get_state(self.team_preset_select)
        if current_selected_team_name and isinstance(current_selected_team_name, str) and current_selected_team_name.lower() != "none":
            await self.fetch_and_update_data_for_team(current_selected_team_name)
        else:
            self._update_sensor_no_team_selected()
            self._schedule_next_refresh(self.refresh_interval_off)

    def terminate(self) -> None:
        """Terminate the app and cleanup resources."""
        if self.timer_handle:
            self.cancel_timer(self.timer_handle)
        
        if self.data_service:
            self.create_task(self._cleanup_service())
        
        self.log_message("NHL Dashboard Manager App Terminated.", level="INFO")

    async def _cleanup_service(self):
        """Async cleanup of the data service."""
        try:
            if self.data_service:
                await self.data_service.close()
                self.log_message("Data service closed successfully.", level="DEBUG")
        except Exception as e:
            self.log_message(f"Error cleaning up data service: {e}", level="ERROR")