# /config/appdaemon/apps/nhl_goal_app.py
import appdaemon.plugins.hass.hassapi as hass
import unicodedata
from typing import Any, Dict, Optional, Tuple, List

# Use shared constants (colors + name normalization)
from nhl_const import TEAM_COLORS, DEFAULT_COLORS_LIST, EVENT_NAME_TO_STANDARD_KEY_MAP


class NhlGoalCelebrations(hass.Hass):
    """
    Classic RGB/transition lightshow (no Zigbee-specific tweaks), with horn + TTS + opponent/penalty handling.
    Win celebrations reuse the exact same lightshow as goals.
    """
    APP_VERSION = "2.11.8-classic"  # Assist list support + NHL API friendly strings

    # Original timings
    RED_CHASE_REPETITIONS = 5
    RED_STROBE_ON_T = 0.10
    RED_STROBE_DIM_T = 0.08
    RED_CHASE_PER_LIGHT_DELAY_T = 0.025
    RED_CHASE_INTER_REP_PAUSE_T = 0.05
    INITIAL_STROBE_CYCLES_PART2 = 8

    BASE_PS_RAPID_ON = 0.10
    BASE_PS_RAPID_DIM_DURATION = 0.06
    PS_RAPID_CYCLES = 6
    BASE_SWEEP_PER_LIGHT = 0.04
    BASE_SWEEP_HOLD_PRIMARY = 0.25
    BASE_SWEEP_POP_SECONDARY_ON = 0.12
    BASE_SWEEP_POP_SECONDARY_OFF = 0.10
    SWEEP_POP_REPETITIONS = 2
    BASE_TRUE_CHASE_PER_LIGHT = 0.05
    BASE_TRUE_CHASE_LIGHT_ON_DURATION = 0.08
    BASE_TRUE_CHASE_PAUSE_AFTER = 0.15
    BASE_PRIMARY_IMPACT_ON = 0.08
    BASE_PRIMARY_IMPACT_OFF = 0.06
    PRIMARY_IMPACT_COUNT = 7
    BASE_FINALE_FLASH_DURATION = 0.18
    FINALE_PST_OFF_DURATION = 0.05
    BASE_FINALE_WHITE_HOLD = 2.0
    BASE_TEAM_COLOR_FINALE_DURATION = 0.4
    BASE_TEAM_COLOR_FLASH_COUNT = 3
    LIGHTSHOW_START_DELAY = 0.4

    _warned_missing_teams = set()

    def initialize(self) -> None:
        self.log_message(f"NHL Goal App Initializing (v{self.APP_VERSION})...", level="INFO")
        self.log_level = self.args.get("log_level", "INFO").upper()

        # Config
        self.goal_event_name = self.args.get("goal_event_name")
        self.opponent_goal_event_name = self.args.get("opponent_goal_event_name")
        self.tts_trigger_event_name = self.args.get("tts_trigger_event_name")
        self.team_win_event_name = self.args.get("team_win_event_name", "nhl_team_win_event")

        self.horn_enabled_boolean = self.args.get("horn_enabled_boolean")
        self.lights_enabled_boolean = self.args.get("lights_enabled_boolean")

        self.horn_volume_input_number = self.args.get("horn_volume_input_number")
        self.light_group = self.args.get("light_group")
        self.media_player_horn = self.args.get("media_player_horn")

        self.horn_base_duration_seconds = float(self.args.get("horn_base_duration_seconds", 21))
        self.horn_fade_out_duration_seconds = float(self.args.get("horn_fade_out_duration_seconds", 3))
        self.horn_fade_step_interval_seconds = float(self.args.get("horn_fade_step_interval_seconds", 1))
        self.horn_media_base_path = self.args.get("horn_media_base_path", "media-source://media_source/local/")
        self.horn_filename_suffix = self.args.get("horn_filename_suffix", " Goal Horn.mp3")

        self.lightshow_target_total_duration_seconds = float(self.args.get("lightshow_target_total_duration_seconds", 28))

        # TTS
        self.tts_enabled_boolean = self.args.get("tts_enabled_boolean")
        self.tts_service = self.args.get("tts_service")
        self.tts_language = self.args.get("tts_language", "en")
        self.tts_volume_input_number = self.args.get("tts_volume_input_number")

        # Sanity
        for name, var in {
            "goal_event_name": self.goal_event_name,
            "opponent_goal_event_name": self.opponent_goal_event_name,
            "tts_trigger_event_name": self.tts_trigger_event_name,
            "light_group": self.light_group,
            "media_player_horn": self.media_player_horn,
        }.items():
            if not var:
                self.log_message(f"CRITICAL ERROR: Essential config '{name}' is missing.", level="ERROR")

        if self.horn_fade_out_duration_seconds > 0 and not (0.1 <= self.horn_fade_step_interval_seconds <= self.horn_fade_out_duration_seconds):
            self.log_message("Config Warning: horn_fade_step_interval impractical. Adjusting to 1s.", level="WARNING")
            self.horn_fade_step_interval_seconds = 1.0

        # Internals
        self.horn_active_timers: List[Any] = []
        self.lightshow_active_timers: List[Any] = []
        self.lightshow_currently_running_for_team: Optional[str] = None

        # Listeners
        if self.goal_event_name:
            self.listen_event(self.goal_event_callback, self.goal_event_name)
        if self.opponent_goal_event_name:
            self.listen_event(self.opponent_goal_callback, self.opponent_goal_event_name)
        if self.tts_trigger_event_name:
            self.listen_event(self.tts_trigger_callback, self.tts_trigger_event_name)
        self.listen_event(self.penalty_event_callback, "nhl_penalty_event")
        if self.team_win_event_name:
            self.listen_event(self.team_win_callback, self.team_win_event_name)

        self.log_message("Event listeners registered.", level="INFO")

    def log_message(self, message: str, level: str = "INFO") -> None:
        self.log(f"[GOAL_APP_V{self.APP_VERSION}] {message}", level=level.upper())

    def _normalize_for_map_lookup(self, team_name_from_event: Optional[str]) -> str:
        if not team_name_from_event:
            return ""
        name = str(team_name_from_event).replace("\uFFFD", "e")
        normalized = unicodedata.normalize("NFD", name)
        stripped = "".join(c for c in normalized if unicodedata.category(c) != "Mn")
        return unicodedata.normalize("NFC", stripped)

    def _tts_delay_after_horn(self) -> float:
        """
        Safe delay so TTS speaks after the horn (and its fade) completes.
        Used for HOME goals; WIN flow keeps original 1s delay.
        """
        try:
            if self.horn_enabled_boolean and self.get_state(self.horn_enabled_boolean) == "on":
                return float(self.horn_base_duration_seconds) + max(0.0, float(self.horn_fade_out_duration_seconds)) + 0.4
        except Exception:
            pass
        return 0.6

    def _read_input_as_float(self, entity_id: Optional[str], default_val: float, min_v: float = 0.0, max_v: float = 1.0) -> float:
        try:
            if not entity_id:
                return default_val
            val = self.get_state(entity_id)
            if isinstance(val, (int, float)):
                f = float(val)
            elif isinstance(val, str) and val.lower() not in ("unknown", "unavailable", ""):
                f = float(val)
            else:
                return default_val
            return max(min_v, min(max_v, f))
        except Exception:
            return default_val

    # ------- Events -------

    def goal_event_callback(self, event_name: str, data: Dict[str, Any], kwargs: Dict[str, Any]) -> None:
        raw_team_name = data.get("team_name")
        self.log_message(f"Goal event '{event_name}' received for team: '{raw_team_name}'", level="INFO")
        if not raw_team_name:
            self.log_message("No team name in event data. Aborting.", level="ERROR")
            return

        standard_key = EVENT_NAME_TO_STANDARD_KEY_MAP.get(self._normalize_for_map_lookup(raw_team_name), raw_team_name)
        if not standard_key or standard_key.lower() in ["none", "unknown"]:
            self.log_message(f"Invalid/missing team key after mapping: '{standard_key}'. Aborting.", level="ERROR")
            return

        # Start horn + lightshow
        self._start_full_celebration(standard_key, data)

        # Speak a short line for HOME team goals (after horn completes)
        try:
            if self.get_state(self.tts_enabled_boolean) == "on":
                my_name = data.get("my_team_name", standard_key)
                scorer = data.get("scorer") or "your team"
                period = data.get("goal_period_ord") or data.get("period") or ""
                when = data.get("goal_time") or data.get("time_remaining") or ""
                my_score = data.get("my_team_score")
                opp_score = data.get("opp_team_score")

                assists_field = data.get("assists")
                if isinstance(assists_field, list):
                    assists_clean = ", ".join(assists_field)
                    if assists_clean:
                        scorer_line = f"Scored by {scorer} (assists: {assists_clean})"
                    else:
                        scorer_line = f"Scored by {scorer}"
                else:
                    scorer_line = f"Scored by {scorer}"

                parts = [f"Goal for the {my_name}!", scorer_line]
                timing = []
                if period:
                    timing.append(f"in the {period}")
                if when:
                    timing.append(f"at {when}")
                if timing:
                    parts.append(" ".join(timing))
                if isinstance(my_score, int) and isinstance(opp_score, int):
                    parts.append(f"Score {my_score} to {opp_score}.")
                tts_msg = " ".join(parts)

                delay_s = self._tts_delay_after_horn()
                self.lightshow_active_timers.append(
                    self.run_in(lambda k: self.fire_event(self.tts_trigger_event_name, tts_message=tts_msg), delay=delay_s)
                )
        except Exception as e:
            self.log_message(f"TTS scheduling error on goal: {e}", level="WARNING")

    def team_win_callback(self, event_name: str, data: Dict[str, Any], kwargs: Dict[str, Any]) -> None:
        """
        Original behavior for WIN:
        - Start celebration (horn + lights).
        - If TTS enabled, speak after 1.0s (no horn-delay math, no volume 'restore').
        """
        raw_team_name = data.get("team_name")
        self.log_message(f"TEAM WIN event '{event_name}' for team: '{raw_team_name}'", level="INFO")
        if not raw_team_name:
            return
        standard_key = EVENT_NAME_TO_STANDARD_KEY_MAP.get(self._normalize_for_map_lookup(raw_team_name), raw_team_name)

        self._start_full_celebration(standard_key, data)

        if self.get_state(self.tts_enabled_boolean) == "on":
            my_name = data.get("my_team_name", standard_key)
            my_sc = data.get("my_team_score", 0)
            opp_sc = data.get("opp_team_score", 0)
            tts_msg = f"That's the end of the game. Your {my_name} win with a final score of {my_sc} to {opp_sc}."
            self.lightshow_active_timers.append(
                self.run_in(lambda k: self.fire_event(self.tts_trigger_event_name, tts_message=tts_msg), delay=1.0)
            )

    def _start_full_celebration(self, standard_key: str, event_data: Dict[str, Any]) -> None:
        """Shared entry for both goal and win so the show is identical."""
        self.cancel_ongoing_celebrations()

        if self.get_state(self.horn_enabled_boolean) == "on":
            self.run_horn_sequence(standard_key)

        if self.get_state(self.lights_enabled_boolean) == "on":
            if standard_key in TEAM_COLORS:
                self.lightshow_active_timers.append(
                    self.run_in(self.start_lightshow_callback, delay=self.LIGHTSHOW_START_DELAY, team_name=standard_key, event_data=event_data or {})
                )
            else:
                if standard_key not in self._warned_missing_teams:
                    self._warned_missing_teams.add(standard_key)
                    self.log_message(f"Team '{standard_key}' not found in TEAM_COLORS. Lightshow skipped.", level="ERROR")

    def opponent_goal_callback(self, event_name: str, data: Dict[str, Any], kwargs: Dict[str, Any]) -> None:
        self.cancel_ongoing_celebrations()
        flash_duration = 0
        if self.get_state(self.lights_enabled_boolean) == "on":
            num_flashes = 4
            flash_duration = num_flashes * 0.4
            for i in range(num_flashes):
                d = i * 0.4
                self.lightshow_active_timers.append(self.run_in(lambda k: self._call_light_service("turn_on", [self.light_group], rgb=[255, 0, 0], brightness_pct=100), d))
                self.lightshow_active_timers.append(self.run_in(lambda k: self._call_light_service("turn_off", [self.light_group]), d + 0.2))
            self.lightshow_active_timers.append(
                self.run_in(lambda k: self._call_light_service("turn_on", [self.light_group], rgb=[255, 255, 255], brightness_pct=100, transition=0.5), flash_duration)
            )

        if self.get_state(self.tts_enabled_boolean) == "on":
            tts_delay = flash_duration + 1.0
            scorer_line = data.get('scorer', 'a player')
            assists = data.get("assists")
            if isinstance(assists, list) and assists:
                scorer_line += f" (assists: {', '.join(assists)})"
            tts_msg = (
                f"{data.get('team_name', 'The opponent')} goal. "
                f"Scored by {scorer_line} with {data.get('time_remaining', 'recently')} remaining in {data.get('period', 'the period')}. "
                f"The score is now {data.get('my_team_score', 0)} to {data.get('opp_team_score', 0)}."
            )
            self.lightshow_active_timers.append(self.run_in(lambda k: self.fire_event(self.tts_trigger_event_name, tts_message=tts_msg), delay=tts_delay))

    def penalty_event_callback(self, event_name: str, data: Dict[str, Any], kwargs: Dict[str, Any]) -> None:
        if self.get_state(self.lights_enabled_boolean) != "on":
            return
        try:
            self.lightshow_active_timers.append(self.run_in(lambda k: self._call_light_service("turn_on", [self.light_group], rgb=[255, 0, 0], brightness_pct=100), 0))
            self.lightshow_active_timers.append(self.run_in(lambda k: self._call_light_service("turn_off", [self.light_group]), 0.15))
            self.lightshow_active_timers.append(self.run_in(lambda k: self._call_light_service("turn_on", [self.light_group], rgb=[255, 0, 0], brightness_pct=100), 0.30))
            self.lightshow_active_timers.append(self.run_in(lambda k: self._call_light_service("turn_on", [self.light_group], rgb=[255, 255, 255], brightness_pct=100, transition=0.4), 0.45))
        except Exception as e:
            self.log_message(f"Penalty flash error: {e}", level="WARNING")

    def tts_trigger_callback(self, event_name: str, data: Dict[str, Any], kwargs: Dict[str, Any]) -> None:
        tts_message = data.get("tts_message")
        if tts_message:
            self._send_tts(tts_message)

    # ------- Horn -------

    def run_horn_sequence(self, team_name: str) -> None:
        if not self.media_player_horn or not self.horn_volume_input_number:
            self.log_message("Horn media player or volume input not configured.", level="ERROR")
            return

        initial_volume = self._read_input_as_float(self.horn_volume_input_number, 0.5, 0.0, 1.0)

        horn_file = f"{team_name}{self.horn_filename_suffix}"
        content_id = f"{self.horn_media_base_path}{horn_file}"

        try:
            self.call_service("media_player/volume_set", entity_id=self.media_player_horn, volume_level=initial_volume)
        except Exception as e:
            self.log_message(f"HORN: Error setting initial volume: {e}", level="ERROR")
            return

        self.horn_active_timers.append(
            self.run_in(
                self.play_horn_media_action,
                0.3,
                media_player=self.media_player_horn,
                content_id=content_id,
                team_name_for_log=team_name,
                initial_volume_for_fade=initial_volume,
                main_play_duration=self.horn_base_duration_seconds,
                num_fade_steps=int(self.horn_fade_out_duration_seconds / self.horn_fade_step_interval_seconds)
                if self.horn_fade_out_duration_seconds > 0 and self.horn_fade_step_interval_seconds > 0
                else 0,
                fade_step_interval_s=self.horn_fade_step_interval_seconds,
            )
        )

    def play_horn_media_action(self, kwargs: Dict[str, Any]) -> None:
        media_player = kwargs["media_player"]
        content_id = kwargs["content_id"]
        initial_vol_for_fade = kwargs["initial_volume_for_fade"]
        main_play_duration = kwargs["main_play_duration"]
        num_fade_steps = kwargs["num_fade_steps"]
        fade_step_interval_s = kwargs["fade_step_interval_s"]

        try:
            self.call_service("media_player/volume_set", entity_id=media_player, volume_level=initial_vol_for_fade)
        except Exception:
            pass

        try:
            self.call_service("media_player/play_media", entity_id=media_player, media_content_id=content_id, media_content_type="audio/mp3")
        except Exception as e:
            self.log_message(f"HORN: Error playing media: {e}", level="ERROR")
            self.horn_active_timers.clear()
            return

        current_fade_delay = main_play_duration
        if num_fade_steps > 0:
            for i in range(num_fade_steps):
                step = i + 1
                volume_target = round(max(0.01, initial_vol_for_fade * (1 - (step / (num_fade_steps + 1)))), 3)
                self.horn_active_timers.append(
                    self.run_in(
                        self.horn_fade_step_action,
                        delay=current_fade_delay,
                        media_player_target=media_player,
                        volume_target=volume_target,
                        step_info=f"{step}/{num_fade_steps}",
                    )
                )
                current_fade_delay += fade_step_interval_s

        self.horn_active_timers.append(self.run_in(self.horn_final_stop_action, delay=current_fade_delay + 0.1, media_player_target=media_player))

    def horn_fade_step_action(self, kwargs: Dict[str, Any]) -> None:
        self.call_service("media_player/volume_set", entity_id=kwargs["media_player_target"], volume_level=kwargs["volume_target"])

    def horn_final_stop_action(self, kwargs: Dict[str, Any]) -> None:
        mp = kwargs["media_player_target"]
        try:
            self.call_service("media_player/volume_set", entity_id=mp, volume_level=0.05)
            self.run_in(lambda k: self.call_service("media_player/media_stop", entity_id=mp), 0.2)
        except Exception:
            pass
        self.horn_active_timers.clear()

    # ------- TTS -------

    def _send_tts(self, message: str) -> None:
        if not self.media_player_horn or not self.tts_service:
            self.log_message("TTS media player or service not configured.", level="WARNING")
            return
        try:
            # Always force volume from input_number.nhl_tts_volume before TTS
            tts_vol = self._read_input_as_float(self.tts_volume_input_number, 0.8, 0.1, 1.0)
            self.call_service("media_player/volume_set", entity_id=self.media_player_horn, volume_level=tts_vol)

            # Speak
            self.call_service(self.tts_service.replace(".", "/"), entity_id=self.media_player_horn, message=message, language=self.tts_language)

            # Re-assert volume shortly after (in case the player nudges it)
            try:
                self.run_in(lambda k: self.call_service("media_player/volume_set", entity_id=self.media_player_horn, volume_level=tts_vol), 0.2)
            except Exception:
                pass

        except Exception as e:
            self.log_message(f"TTS error on {self.media_player_horn}: {e}", level="ERROR")

    # ------- Light helpers & show -------

    def _get_team_colors_safe(self, team_name: str) -> Tuple[Dict[str, int], Dict[str, int], Dict[str, int]]:
        team_colors_list = TEAM_COLORS.get(team_name)
        if not team_colors_list:
            if team_name not in self._warned_missing_teams:
                self._warned_missing_teams.add(team_name)
                self.log_message(f"WARNING: No color mapping found for '{team_name}'. Using defaults.", level="WARNING")
            team_colors_list = DEFAULT_COLORS_LIST
        primary = team_colors_list[0] if len(team_colors_list) > 0 else DEFAULT_COLORS_LIST[0]
        secondary = team_colors_list[1] if len(team_colors_list) > 1 else DEFAULT_COLORS_LIST[1]
        tertiary = team_colors_list[2] if len(team_colors_list) > 2 else secondary
        return primary, secondary, tertiary

    def _call_light_service(self, service: str, entities: List[str], rgb: Optional[List[int]] = None, brightness_pct: Optional[int] = None, transition: float = 0.0) -> None:
        if not entities:
            return
        data: Dict[str, Any] = {"entity_id": entities, "transition": transition}
        if service == "turn_on":
            if rgb is not None:
                data["rgb_color"] = rgb
            if brightness_pct is not None:
                data["brightness_pct"] = int(max(1, min(100, brightness_pct)))
        try:
            self.call_service(f"light/{service}", **data)
        except Exception as e:
            self.log_message(f"Exception in _call_light_service ({service}): {e}", level="WARNING")

    def start_lightshow_callback(self, kwargs: Dict[str, Any]) -> None:
        team_name = kwargs.get("team_name")
        event_data = kwargs.get("event_data", {}) or {}
        if team_name:
            self.start_main_lightshow_sequence(team_name, event_data)

    def start_main_lightshow_sequence(self, team_name: str, event_data: Dict[str, Any]) -> None:
        self.lightshow_currently_running_for_team = team_name
        self.log_message(f"LIGHTSHOW: Starting v{self.APP_VERSION} for '{team_name}'. Target: {self.lightshow_target_total_duration_seconds}s", level="INFO")

        primary_color, secondary_color, tertiary_color = self._get_team_colors_safe(team_name)
        p_rgb = [primary_color["r"], primary_color["g"], primary_color["b"]]
        s_rgb = [secondary_color["r"], secondary_color["g"], secondary_color["b"]]
        t_rgb = [tertiary_color["r"], tertiary_color["g"], tertiary_color["b"]]
        w_rgb = [255, 255, 255]
        dark_red_rgb = [139, 0, 0]
        bright_red_rgb = [255, 0, 0]

        light_targets_group = self.light_group

        individual_light_entities: List[str] = []
        try:
            group_state = self.get_state(self.light_group, attribute="all")
            if group_state and "attributes" in group_state and "entity_id" in group_state["attributes"]:
                individual_light_entities = group_state["attributes"]["entity_id"]
        except Exception:
            pass
        if not individual_light_entities:
            individual_light_entities = [self.light_group]

        num_individual_lights = len(individual_light_entities)
        if not light_targets_group:
            self.log_message("LIGHTSHOW: No light_group configured. Aborting.", level="ERROR")
            return

        timers = self.lightshow_active_timers
        current_delay = 0.0

        # Red chase
        if num_individual_lights > 1:
            for rep in range(self.RED_CHASE_REPETITIONS):
                for le in individual_light_entities:
                    timers.append(self.run_in(lambda k, le_arg=le: self._call_light_service("turn_on", [le_arg], rgb=dark_red_rgb, brightness_pct=80), current_delay))
                    current_delay += self.RED_CHASE_PER_LIGHT_DELAY_T
                timers.append(self.run_in(lambda k: self._call_light_service("turn_on", individual_light_entities, rgb=bright_red_rgb, brightness_pct=100), current_delay))
                current_delay += self.RED_CHASE_PER_LIGHT_DELAY_T * num_individual_lights
                if rep < self.RED_CHASE_REPETITIONS - 1:
                    timers.append(self.run_in(lambda k: self._call_light_service("turn_on", individual_light_entities, rgb=dark_red_rgb, brightness_pct=10), current_delay))
                    current_delay += self.RED_CHASE_INTER_REP_PAUSE_T
        else:
            for _ in range(self.RED_CHASE_REPETITIONS * 2):
                timers.append(self.run_in(lambda k: self._call_light_service("turn_on", [light_targets_group], rgb=bright_red_rgb, brightness_pct=100), current_delay))
                current_delay += 0.15

        # Red strobe
        for _ in range(self.INITIAL_STROBE_CYCLES_PART2):
            timers.append(self.run_in(lambda k: self._call_light_service("turn_on", [light_targets_group], rgb=bright_red_rgb, brightness_pct=100), current_delay))
            current_delay += self.RED_STROBE_ON_T
            timers.append(self.run_in(lambda k: self._call_light_service("turn_on", [light_targets_group], rgb=bright_red_rgb, brightness_pct=10), current_delay))
            current_delay += self.RED_STROBE_DIM_T

        current_fixed_duration_calculated = current_delay
        self.log_message(f"LIGHTSHOW: Red Light Sequence duration: {current_fixed_duration_calculated:.2f}s", level="DEBUG")

        # Time scaling
        original_variable_duration = (
            (self.PS_RAPID_CYCLES * (self.BASE_PS_RAPID_ON + self.BASE_PS_RAPID_DIM_DURATION) * 2)
            + (
                self.SWEEP_POP_REPETITIONS
                * (
                    (num_individual_lights * self.BASE_SWEEP_PER_LIGHT if num_individual_lights > 1 else 0.15)
                    + self.BASE_SWEEP_HOLD_PRIMARY
                    + self.BASE_SWEEP_POP_SECONDARY_ON
                    + self.BASE_SWEEP_POP_SECONDARY_OFF
                )
            )
            + (
                (num_individual_lights * self.BASE_TRUE_CHASE_PER_LIGHT) * 2
                + self.BASE_TRUE_CHASE_PAUSE_AFTER * 2
                if num_individual_lights > 1
                else (self.BASE_PS_RAPID_ON + self.BASE_PS_RAPID_DIM_DURATION) * 4
            )
            + (self.PRIMARY_IMPACT_COUNT * (self.BASE_PRIMARY_IMPACT_ON + self.BASE_PRIMARY_IMPACT_OFF))
            + (self.BASE_FINALE_FLASH_DURATION * 3 + (self.FINALE_PST_OFF_DURATION * 3))
            + (self.BASE_TEAM_COLOR_FINALE_DURATION * self.BASE_TEAM_COLOR_FLASH_COUNT)
            + self.BASE_FINALE_WHITE_HOLD
        )
        target_variable_duration = self.lightshow_target_total_duration_seconds - current_fixed_duration_calculated
        time_scale_factor = 1.0
        if original_variable_duration > 0 and target_variable_duration > 0:
            time_scale_factor = target_variable_duration / original_variable_duration
        time_scale_factor = max(0.1, min(time_scale_factor, 5.0))
        self.log_message(f"LIGHTSHOW: Var part est={original_variable_duration:.2f}s, target={target_variable_duration:.2f}s, scale={time_scale_factor:.3f}", level="DEBUG")

        # Team color segment
        if target_variable_duration >= 0.2:
            for _ in range(self.PS_RAPID_CYCLES):
                timers.append(self.run_in(lambda k: self._call_light_service("turn_on", [light_targets_group], rgb=p_rgb, brightness_pct=100), current_delay))
                current_delay += self.BASE_PS_RAPID_ON * time_scale_factor
                timers.append(self.run_in(lambda k: self._call_light_service("turn_on", [light_targets_group], rgb=p_rgb, brightness_pct=20), current_delay))
                current_delay += self.BASE_PS_RAPID_DIM_DURATION * time_scale_factor

                timers.append(self.run_in(lambda k: self._call_light_service("turn_on", [light_targets_group], rgb=s_rgb, brightness_pct=100), current_delay))
                current_delay += self.BASE_PS_RAPID_ON * time_scale_factor
                timers.append(self.run_in(lambda k: self._call_light_service("turn_on", [light_targets_group], rgb=s_rgb, brightness_pct=20), current_delay))
                current_delay += self.BASE_PS_RAPID_DIM_DURATION * time_scale_factor

            current_delay += 0.12 * time_scale_factor

            for _ in range(self.SWEEP_POP_REPETITIONS):
                if num_individual_lights > 1:
                    for le in individual_light_entities:
                        timers.append(self.run_in(lambda k, le_arg=le: self._call_light_service("turn_on", [le_arg], rgb=p_rgb, brightness_pct=100), current_delay))
                        current_delay += self.BASE_SWEEP_PER_LIGHT * time_scale_factor
                else:
                    timers.append(self.run_in(lambda k: self._call_light_service("turn_on", [light_targets_group], rgb=p_rgb, brightness_pct=100), current_delay))
                    current_delay += 0.15 * time_scale_factor

                timers.append(self.run_in(lambda k: self._call_light_service("turn_on", [light_targets_group], rgb=s_rgb, brightness_pct=100), current_delay))
                current_delay += self.BASE_SWEEP_POP_SECONDARY_ON * time_scale_factor
                timers.append(self.run_in(lambda k: self._call_light_service("turn_off", [light_targets_group]), current_delay))
                current_delay += self.BASE_SWEEP_POP_SECONDARY_OFF * time_scale_factor

            current_delay += 0.12 * time_scale_factor

            if num_individual_lights > 1:
                for color_to_chase, rev_dir in [(p_rgb, False), (s_rgb, True)]:
                    chase_targets = list(individual_light_entities)
                    if rev_dir:
                        chase_targets.reverse()
                    for le in chase_targets:
                        timers.append(self.run_in(lambda k, le_arg=le, cc_arg=color_to_chase: self._call_light_service("turn_on", [le_arg], rgb=cc_arg, brightness_pct=100), current_delay))
                        timers.append(self.run_in(lambda k, le_arg=le: self._call_light_service("turn_off", [le_arg]), current_delay + self.BASE_TRUE_CHASE_LIGHT_ON_DURATION * time_scale_factor))
                        current_delay += self.BASE_TRUE_CHASE_PER_LIGHT * time_scale_factor
                    current_delay += self.BASE_TRUE_CHASE_PAUSE_AFTER * time_scale_factor
            else:
                for _ in range(2):
                    timers.append(self.run_in(lambda k: self._call_light_service("turn_on", [light_targets_group], rgb=p_rgb, brightness_pct=100), current_delay))
                    current_delay += self.BASE_PS_RAPID_ON * time_scale_factor
                    timers.append(self.run_in(lambda k: self._call_light_service("turn_off", [light_targets_group]), current_delay))
                    current_delay += self.BASE_PS_RAPID_DIM_DURATION * time_scale_factor
                    timers.append(self.run_in(lambda k: self._call_light_service("turn_on", [light_targets_group], rgb=s_rgb, brightness_pct=100), current_delay))
                    current_delay += self.BASE_PS_RAPID_ON * time_scale_factor
                    timers.append(self.run_in(lambda k: self._call_light_service("turn_off", [light_targets_group]), current_delay))
                    current_delay += self.BASE_PS_RAPID_DIM_DURATION * time_scale_factor

            current_delay += 0.12 * time_scale_factor

            for _ in range(self.PRIMARY_IMPACT_COUNT):
                timers.append(self.run_in(lambda k: self._call_light_service("turn_on", [light_targets_group], rgb=p_rgb, brightness_pct=100), current_delay))
                current_delay += self.BASE_PRIMARY_IMPACT_ON * time_scale_factor
                timers.append(self.run_in(lambda k: self._call_light_service("turn_off", [light_targets_group]), current_delay))
                current_delay += self.BASE_PRIMARY_IMPACT_OFF * time_scale_factor

            current_delay += 0.12 * time_scale_factor

            # Triple flash
            for color in [p_rgb, s_rgb, t_rgb]:
                timers.append(self.run_in(lambda k, c_arg=color: self._call_light_service("turn_on", [light_targets_group], rgb=c_arg, brightness_pct=100), current_delay))
                current_delay += self.BASE_FINALE_FLASH_DURATION * time_scale_factor
                timers.append(self.run_in(lambda k: self._call_light_service("turn_off", [light_targets_group]), current_delay))
                current_delay += self.FINALE_PST_OFF_DURATION * time_scale_factor

            # Alternating team color finale
            team_colors_to_flash_finale = [p_rgb, s_rgb]
            for _ in range(self.BASE_TEAM_COLOR_FLASH_COUNT):
                for color in team_colors_to_flash_finale:
                    timers.append(self.run_in(lambda k, c_arg=color: self._call_light_service("turn_on", [light_targets_group], rgb=c_arg, brightness_pct=100), current_delay))
                    current_delay += self.BASE_TEAM_COLOR_FINALE_DURATION * time_scale_factor
                    timers.append(self.run_in(lambda k: self._call_light_service("turn_off", [light_targets_group]), current_delay))
                    current_delay += 0.08 * time_scale_factor

        # Final white hold (leave ON)
        timers.append(self.run_in(lambda k: self._call_light_service("turn_on", [light_targets_group], rgb=w_rgb, brightness_pct=100), current_delay))
        current_delay += self.BASE_FINALE_WHITE_HOLD * time_scale_factor

        # Force-finish: cancel any straggler timers and keep lights on white
        timers.append(self.run_in(self._force_finish_white, current_delay, light_group=light_targets_group))

        self.log_message(f"LIGHTSHOW: Scheduled timers. Total est. duration ≈ {current_delay:.2f}s", level="DEBUG")

    def _force_finish_white(self, kwargs: Dict[str, Any]) -> None:
        """Cancels remaining timers and enforces solid white at the end of any celebration."""
        for h in list(self.lightshow_active_timers):
            try:
                if self.timer_running(h):
                    self.cancel_timer(h)
            except Exception:
                pass
        self.lightshow_active_timers.clear()

        lg = kwargs.get("light_group") or self.light_group
        self._call_light_service("turn_on", [lg], rgb=[255, 255, 255], brightness_pct=100, transition=0.2)
        self.lightshow_currently_running_for_team = None
        self.log_message("LIGHTSHOW: Finished and locked to white.", level="INFO")

    # ------- Cancel helpers -------

    def cancel_ongoing_celebrations(self) -> None:
        for h in list(self.lightshow_active_timers):
            try:
                if self.timer_running(h):
                    self.cancel_timer(h)
            except Exception:
                pass
        self.lightshow_active_timers.clear()

        for h in list(self.horn_active_timers):
            try:
                if self.timer_running(h):
                    self.cancel_timer(h)
            except Exception:
                pass
        self.horn_active_timers.clear()
        try:
            if self.media_player_horn:
                self.call_service("media_player/media_stop", entity_id=self.media_player_horn)
        except Exception:
            pass