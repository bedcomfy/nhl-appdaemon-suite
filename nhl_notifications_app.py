import appdaemon.plugins.hass.hassapi as hass
import datetime
import copy
import re
import time
import unicodedata
from typing import Tuple, Optional, Dict, Any, List

from nhl_const import (
    NHL_TEAM_ABBREV_TO_FULL_NAME_MAP,
    NHL_TEAM_NAME_TO_ABBREV_MAP,
    PRESET_TO_API_STYLE_NAME_MAP,
    API_TO_STANDARD_TEAM_NAME_MAP,
    STANDARD_NAME_TO_PUSHOVER_SOUND_MAP
)


class NhlGameNotifications(hass.Hass):
    APP_VERSION = "4.8.9"  # Full template coverage everywhere

    def initialize(self):
        self.log_level = self.args.get("log_level", "INFO").upper()
        self.log_message(f"NHL Game Notifications App Initializing (v{self.APP_VERSION})...", level="INFO")

        # Config
        self.pushover_main_notifier = self.args.get("pushover_main_notifier")
        self.dev_notifier_target = self.args.get("dev_notifier_target")
        self.team_notification_preset_select = self.args.get("team_notification_preset_select")
        self.dashboard_sensor_entity_id = self.args.get("dashboard_sensor_entity_id")
        self.nhl_api_sensor_entity_id = (self.args.get("nhl_api_sensor_entity_id") or "sensor.nhl_api_active_team").strip()

        self.test_notification_boolean = self.args.get("test_notification_boolean")
        self.test_opponent_goal_boolean = self.args.get("test_opponent_goal_boolean")
        self.test_team_win_boolean = self.args.get("test_team_win_boolean")

        self.horn_enabled_boolean_for_preset_notif = self.args.get("horn_enabled_boolean_for_preset_notif")
        self.lights_enabled_boolean_for_preset_notif = self.args.get("lights_enabled_boolean_for_preset_notif")
        self.tts_enabled_boolean_for_preset_notif = self.args.get("tts_enabled_boolean_for_preset_notif")

        self.goal_event_to_fire = self.args.get("goal_event_to_fire", "nhl_goal_event_appdaemon")
        self.opponent_goal_event_to_fire = self.args.get("opponent_goal_event_to_fire")
        self.team_win_event_to_fire = self.args.get("team_win_event_to_fire", "nhl_team_win_event")
        self.celebrate_win_only_if_home = bool(self.args.get("celebrate_win_only_if_home", True))

        # Tuning
        self.coalesce_goal_seconds = float(self.args.get("coalesce_goal_seconds", 1.3))
        self.coalesce_penalty_seconds = float(self.args.get("coalesce_penalty_seconds", 2.0))
        self.suppress_after_scoreboard_goal_seconds = float(self.args.get("suppress_after_scoreboard_goal_seconds", 5.0))

        # Template overrides + broadcast delay
        self.broadcast_delay_input_number = self.args.get("broadcast_delay_input_number")
        self.broadcast_delay_seconds = float(self.args.get("broadcast_delay_seconds", 0.0))
        self.texts_app_name = self.args.get("texts_app")
        self.texts_app = None
        if self.texts_app_name:
            self.log_message(f"Template overrides enabled via '{self.texts_app_name}'.", level="INFO")

        # Internal state
        self.internal_prev_home_score = 0
        self.internal_prev_away_score = 0
        self.internal_prev_game_id_for_score_tracking = None
        self.internal_prev_last_play = None
        self.internal_prev_last_event_sig = None
        self.internal_prev_last_event_id = None
        self.internal_win_fired_game_id = None
        self.internal_start_fired_game_id = None

        # API tracking & coalescing
        self.nhl_api_goal_ids_processed: set = set()
        self.pending_event_timers: Dict[str, Any] = {}
        self.pending_event_payloads: Dict[str, Dict] = {}
        self.fired_event_ids: set = set()

        # Scoreboard fallback
        self.sb_goal_suppress_until_ts: float = 0.0
        self.sb_last_fired_home_away: Optional[Tuple[int, int]] = None
        self.pending_sb_goal_timers: Dict[Tuple[int, int], Any] = {}

        self.player_team_map: Dict[str, str] = {}
        self.selected_team_warning_logged = False

        for name, var in {
            "pushover_main_notifier": self.pushover_main_notifier,
            "team_notification_preset_select": self.team_notification_preset_select,
            "dashboard_sensor_entity_id": self.dashboard_sensor_entity_id
        }.items():
            if var is None:
                self.log_message(f"CRITICAL ERROR: Essential config '{name}' is missing.", level="ERROR")

        if self.dashboard_sensor_entity_id:
            self.create_task(self._async_initial_state_load())
            self.listen_state(self.dashboard_sensor_change_callback, self.dashboard_sensor_entity_id, attribute="all")
            self.log_message(f"Listening for state changes on {self.dashboard_sensor_entity_id}", level="INFO")

        if self.nhl_api_sensor_entity_id:
            if self.entity_exists(self.nhl_api_sensor_entity_id):
                self.listen_state(self.nhl_api_sensor_change_callback, self.nhl_api_sensor_entity_id, attribute="all")
                self.log_message(f"NHL API sensor bridge enabled ({self.nhl_api_sensor_entity_id}).", level="INFO")
            else:
                self.log_message(
                    f"{self.nhl_api_sensor_entity_id} not present yet; retrying listener registration in 10s.",
                    level="WARNING"
                )
                self.run_in(self._deferred_api_sensor_listener, 10)

        if self.team_notification_preset_select:
            self.listen_state(self.team_preset_change_callback, self.team_notification_preset_select)

        if self.test_notification_boolean:
            self.listen_state(self.test_notification_callback, self.test_notification_boolean, new="on", is_opponent_test=False)
        if self.test_opponent_goal_boolean:
            self.listen_state(self.test_notification_callback, self.test_opponent_goal_boolean, new="on", is_opponent_test=True)
        if self.test_team_win_boolean:
            self.listen_state(self.test_win_callback, self.test_team_win_boolean, new="on")

        for boolean in [
            self.lights_enabled_boolean_for_preset_notif,
            self.horn_enabled_boolean_for_preset_notif,
            self.tts_enabled_boolean_for_preset_notif
        ]:
            if boolean:
                self.listen_state(self.control_toggle_callback, boolean)

    # -----------------------------------------------------------------------
    # Helper utilities
    # -----------------------------------------------------------------------

    def _deferred_api_sensor_listener(self, _args):
        if self.entity_exists(self.nhl_api_sensor_entity_id):
            self.listen_state(self.nhl_api_sensor_change_callback, self.nhl_api_sensor_entity_id, attribute="all")
            self.log_message(f"NHL API sensor bridge enabled ({self.nhl_api_sensor_entity_id}).", level="INFO")
        else:
            self.log_message(
                f"{self.nhl_api_sensor_entity_id} still missing; retrying listener registration in 10s.",
                level="WARNING"
            )
            self.run_in(self._deferred_api_sensor_listener, 10)

    def log_message(self, message: str, level: str = "INFO") -> None:
        self.log(f"[NOTIFICATIONS_V{self.APP_VERSION}] {message}", level=level.upper())

    def _get_texts_app(self):
        if not self.texts_app_name:
            return None
        if self.texts_app is None:
            try:
                self.texts_app = self.get_app(self.texts_app_name)
            except Exception:
                self.texts_app = None
        return self.texts_app

    def _render_template(self, key: str, default_template: str, **kwargs) -> str:
        template_app = self._get_texts_app()
        if template_app and hasattr(template_app, "render"):
            try:
                return template_app.render(key, default_template, **kwargs)
            except Exception as e:
                self.log_message(f"Template render error for '{key}': {e}", level="WARNING")
        try:
            return (default_template or "").format(**kwargs)
        except Exception:
            return default_template or ""

    def _get_selected_team_abbrev(self) -> Optional[str]:
        preset_state = self.get_state(self.team_notification_preset_select)
        if preset_state in (None, "unknown", "unavailable"):
            if not self.selected_team_warning_logged:
                self.log_message("Selected team preset unavailable; goal tracking paused until it resolves.", level="WARNING")
                self.selected_team_warning_logged = True
            return None
        preset = str(preset_state)
        api_style = PRESET_TO_API_STYLE_NAME_MAP.get(preset, preset)
        abbrev = NHL_TEAM_NAME_TO_ABBREV_MAP.get(api_style)
        if abbrev:
            self.selected_team_warning_logged = False
        return abbrev

    def _get_broadcast_delay(self) -> float:
        try:
            if self.broadcast_delay_input_number:
                val = self.get_state(self.broadcast_delay_input_number)
                if isinstance(val, (int, float)):
                    return max(0.0, min(120.0, float(val)))
                if isinstance(val, str) and val.lower() not in ("unknown", "unavailable", "", "none"):
                    return max(0.0, min(120.0, float(val)))
        except Exception:
            pass
        return max(0.0, min(120.0, float(self.broadcast_delay_seconds)))

    async def _async_initial_state_load(self):
        initial_state_obj = await self.get_state(self.dashboard_sensor_entity_id, attribute="all")
        self._process_initial_state(initial_state_obj)

    def _process_initial_state(self, initial_state_obj):
        self._clear_pending_events()
        if initial_state_obj and isinstance(initial_state_obj, dict) and initial_state_obj.get("state") != "unavailable":
            attrs = initial_state_obj.get("attributes", {})
            self.internal_prev_home_score = attrs.get("home_score", 0)
            self.internal_prev_away_score = attrs.get("away_score", 0)
            self.internal_prev_game_id_for_score_tracking = attrs.get("game_id")
            self.internal_prev_last_play = attrs.get("last_play", "N/A")
            self.internal_prev_last_event_sig = None
            self.internal_prev_last_event_id = None
            self.internal_win_fired_game_id = None
            self.internal_start_fired_game_id = None
        else:
            self.internal_prev_home_score = 0
            self.internal_prev_away_score = 0
            self.internal_prev_game_id_for_score_tracking = None
            self.internal_prev_last_play = "N/A"
            self.internal_prev_last_event_sig = None
            self.internal_prev_last_event_id = None
            self.internal_win_fired_game_id = None
            self.internal_start_fired_game_id = None

    def _clear_pending_events(self):
        for eid, handle in list(self.pending_event_timers.items()):
            try:
                if self.timer_running(handle):
                    self.cancel_timer(handle)
            except Exception:
                pass
        self.pending_event_timers.clear()
        self.pending_event_payloads.clear()
        self.fired_event_ids.clear()
        self.nhl_api_goal_ids_processed.clear()

        for score_sig, handle in list(self.pending_sb_goal_timers.items()):
            try:
                if self.timer_running(handle):
                    self.cancel_timer(handle)
            except Exception:
                pass
            self.pending_sb_goal_timers.pop(score_sig, None)

        self.sb_goal_suppress_until_ts = 0.0
        self.sb_last_fired_home_away = None

    def _strip_html(self, s):
        return re.sub(r'<[^>]+>', '', s or "").replace("&nbsp;", " ").replace("&amp;", "&")

    def _cleanup_player_display(self, name: Any) -> str:
        if not name:
            return ""
        text = str(name).strip().strip("'\" ")
        text = re.sub(r"\s+#\d+$", "", text)
        text = re.sub(r"\s+\(.*?\)$", "", text)
        return text

    def _player_for_tts(self, name: Any) -> str:
        display = self._cleanup_player_display(name)
        return display or "a player"

    def _normalize_name(self, name: Any) -> str:
        if not name:
            return ""
        cleaned = self._cleanup_player_display(name)
        normalized = unicodedata.normalize("NFKD", cleaned).encode("ascii", "ignore").decode("ascii")
        return re.sub(r'[^a-z0-9]', '', normalized.lower())

    def _parse_scorer_from_last_play(self, last_play: str) -> Tuple[str, Optional[str]]:
        match = re.search(r"Goal:\s*([^(\n]+?)\s*\(([A-Z]{3})\)", last_play, re.IGNORECASE)
        if match:
            return match.group(1).strip(), match.group(2).upper()
        scorer_match = re.search(r"Goal:\s*([^(\n]+)", last_play)
        if scorer_match:
            return scorer_match.group(1).strip(), None
        return "a player", None

    def _remember_players_for_team(self, names: List[str], team_abbr: Optional[str]):
        if not team_abbr:
            return
        team_abbr = team_abbr.upper()
        for name in names:
            key = self._normalize_name(name)
            if key:
                self.player_team_map[key] = team_abbr

    # -----------------------------------------------------------------------
    # Template-driven goal handling
    # -----------------------------------------------------------------------

    def _build_goal_texts(
        self,
        *,
        is_my_goal: bool,
        scorer: str,
        assists: List[str],
        shot_type: Optional[str],
        strength: Optional[str],
        period_ord: Optional[str],
        when: Optional[str],
        my_team_name: Optional[str],
        my_team_abbr: str,
        opp_team_name: Optional[str],
        opp_team_abbr: str,
        my_score: int,
        opp_score: int,
        game_url: Optional[str],
        source: str
    ):
        assists = assists or []
        scorer_display = scorer or "a player"
        assists_line = ", ".join([a for a in assists if a])
        shot_info_bits = [bit for bit in [shot_type, strength] if bit]
        shot_info = " | ".join(shot_info_bits)
        goal_line = f"{shot_info} goal by <b>{scorer_display}</b>" if shot_info else f"Goal by <b>{scorer_display}</b>"
        assist_line_section = f"<br>Assists: {assists_line}" if assists_line else ""
        period_ord = period_ord or ""
        when = when or ""
        time_line = f"{period_ord} • {when}" if (period_ord or when) else ""
        score_line = f"{my_team_abbr} {my_score} - {opp_team_abbr} {opp_score}"
        scorer_tts = self._player_for_tts(scorer_display)
        assists_tts = [self._player_for_tts(a) for a in assists if a]

        tts_sentences: List[str] = []
        if shot_info_bits:
            descriptor = " and ".join(bit.lower() for bit in shot_info_bits)
            tts_sentences.append(f"{scorer_tts} scores on a {descriptor}")
        else:
            tts_sentences.append(f"{scorer_tts} scores")

        if assists_tts:
            if len(assists_tts) == 1:
                tts_sentences.append(f"assisted by {assists_tts[0]}")
            else:
                assists_phrase = ", ".join(assists_tts[:-1]) + f", and {assists_tts[-1]}"
                tts_sentences.append(f"assisted by {assists_phrase}")

        lead_sentence = f"{my_team_abbr} leads {score_line}" if is_my_goal else f"{opp_team_abbr} takes the lead {score_line}"
        tts_sentences.append(lead_sentence)
        goal_tts_sentence = ". ".join(tts_sentences)

        context = {
            "scorer": scorer_display,
            "scorer_tts": scorer_tts,
            "assists": assists,
            "assists_tts": assists_tts,
            "assists_joined": assists_line,
            "assist_line_section": assist_line_section,
            "shot_type": shot_type or "",
            "strength": strength or "",
            "shot_info": shot_info,
            "period_ord": period_ord,
            "time": when,
            "time_line": time_line,
            "score_line": score_line,
            "goal_line": goal_line,
            "my_team_name": my_team_name or "",
            "my_team_name_upper": (my_team_name or "").upper(),
            "my_team_abbr": my_team_abbr,
            "opp_team_name": opp_team_name or "",
            "opp_team_abbr": opp_team_abbr,
            "my_score": my_score,
            "opp_score": opp_score,
            "is_my_goal": is_my_goal,
            "goal_tts_sentence": goal_tts_sentence,
            "game_url": game_url,
            "source": source,
        }

        default_title_template = "🚨 GOAL! {my_team_name_upper}! 🚨" if is_my_goal else "🥅 Opponent Goal: {opp_team_name}"
        default_body_template = "<b>{score_line}</b><br>{time_line}<br>{goal_line}{assist_line_section}"
        default_tts_template = "{goal_tts_sentence}"
        return context, default_title_template, default_body_template, default_tts_template

    def _dispatch_goal_notifications(self, context: Dict[str, Any],
                                     default_title_template: str,
                                     default_body_template: str,
                                     default_tts_template: str) -> str:
        if context["is_my_goal"]:
            title_key = "goal_title"
            body_key = "goal_body"
            tts_key = "goal_tts"
            pushover_sound = self._generate_pushover_sound_name(context.get("my_team_name", ""))
            priority = 1
            attachment = context.get("my_logo")
            url_title = "🚨 View Goal Details!"
        else:
            title_key = "opponent_goal_title"
            body_key = "opponent_goal_body"
            tts_key = "opponent_goal_tts"
            pushover_sound = "pushover"
            priority = 0
            attachment = context.get("opp_logo")
            url_title = "View Game"

        title = self._render_template(title_key, default_title_template, **context)
        body = self._render_template(body_key, default_body_template, **context)
        tts_phrase = self._render_template(tts_key, default_tts_template, **context)
        context["tts_phrase"] = tts_phrase

        if self.pushover_main_notifier:
            data: Dict[str, Any] = {
                "html": 1,
                "priority": priority,
                "sound": pushover_sound
            }
            if context.get("game_url"):
                data["url"] = context["game_url"]
                data["url_title"] = url_title
            if attachment:
                data["attachment"] = attachment
            self.send_notification(self.pushover_main_notifier, title, body, data)

        return tts_phrase

    def _emit_goal_events(self,
                          context: Dict[str, Any],
                          scorer: str,
                          assists: List[str],
                          shot_type: Optional[str],
                          strength: Optional[str],
                          period_ord: Optional[str],
                          when: Optional[str]):
        if context["is_my_goal"]:
            event = {
                "team_name": API_TO_STANDARD_TEAM_NAME_MAP.get(context["my_team_name"], context["my_team_name"]),
                "my_team_name": context["my_team_name"],
                "my_team_abbr": context["my_team_abbr"],
                "opp_team_name": context["opp_team_name"],
                "opp_team_abbr": context["opp_team_abbr"],
                "scorer": scorer,
                "scorer_tts": context.get("scorer_tts"),
                "assists": assists,
                "assists_tts": context.get("assists_tts"),
                "goal_time": when or None,
                "goal_period_ord": period_ord or None,
                "goal_strength": strength,
                "shot_type": shot_type,
                "my_team_score": context.get("my_score"),
                "opp_team_score": context.get("opp_score"),
                "period": period_ord,
                "time_remaining": when,
                "tts_phrase": context.get("tts_phrase"),
                "source": context.get("source")
            }
            self.fire_event(self.goal_event_to_fire, **event)
        else:
            if not self.opponent_goal_event_to_fire:
                return
            event = {
                "team_name": context["opp_team_name"],
                "scorer": scorer,
                "scorer_tts": context.get("scorer_tts"),
                "assists": assists,
                "assists_tts": context.get("assists_tts"),
                "my_team_score": context.get("my_score"),
                "opp_team_score": context.get("opp_score"),
                "period": period_ord,
                "time_remaining": when,
                "shot_type": shot_type,
                "goal_strength": strength,
                "tts_phrase": context.get("tts_phrase"),
                "source": context.get("source")
            }
            self.fire_event(self.opponent_goal_event_to_fire, **event)

    def send_notification(self, notifier_target_name, title, message, data=None):
        if not notifier_target_name:
            self.log_message("Notifier target name not provided.", level="ERROR")
            return
        if notifier_target_name == "persistent_dev_log":
            text = f"{title}\n-----------------\n{self._strip_html(message)}"
            if len(text) > 2000:
                text = text[:1997] + "..."
            try:
                self.call_service(
                    "persistent_notification/create",
                    message=text,
                    title="NHL Diag Log",
                    notification_id=f"nhl_dev_log_{datetime.datetime.now().strftime('%H%M%S%f')}"
                )
            except Exception as e:
                self.log_message(f"Error sending Persistent Notification: {e}", level="ERROR")
        elif notifier_target_name.startswith("notify."):
            service = notifier_target_name.replace('.', '/')
            payload = {"title": title, "message": message}
            if data:
                payload["data"] = data
            try:
                self.call_service(service, **payload)
            except Exception as e:
                self.log_message(f"Error sending HA Notification via {service}: {e}", level="ERROR")
        else:
            self.log_message(f"Unknown notifier_target_name format: '{notifier_target_name}'.", level="ERROR")

    def _generate_pushover_sound_name(self, api_team_name: str) -> str:
        if not api_team_name:
            return "hockey_goal"
        standard_name = API_TO_STANDARD_TEAM_NAME_MAP.get(api_team_name, api_team_name)
        return STANDARD_NAME_TO_PUSHOVER_SOUND_MAP.get(standard_name, "hockey_goal")

    # -----------------------------------------------------------------------
    # UI toggle notifications
    # -----------------------------------------------------------------------

    def control_toggle_callback(self, entity, attribute, old, new, kwargs):
        if old == new:
            return
        features = []
        if self.lights_enabled_boolean_for_preset_notif and self.get_state(self.lights_enabled_boolean_for_preset_notif) == 'on':
            features.append('Lights')
        if self.horn_enabled_boolean_for_preset_notif and self.get_state(self.horn_enabled_boolean_for_preset_notif) == 'on':
            features.append('Horn')
        if self.tts_enabled_boolean_for_preset_notif and self.get_state(self.tts_enabled_boolean_for_preset_notif) == 'on':
            features.append('TTS')
        status_text = ' & '.join(features) + ' Enabled' if features else 'All Disabled'

        if self.pushover_main_notifier:
            tmpl = {"status_text": status_text}
            title = self._render_template("controls_title", "🏒 NHL Controls Updated", **tmpl)
            message = self._render_template("controls_body", "Celebration status is now: <b>{status_text}</b>.", **tmpl)
            self.send_notification(self.pushover_main_notifier, title, message, {"html": 1, "priority": -2, "sound": "pushover"})

    def team_preset_change_callback(self, entity, attribute, old_state_val, new_state_val, kwargs):
        if old_state_val == new_state_val or not new_state_val or new_state_val in ["unavailable", "unknown", "None", ""]:
            return
        self.internal_prev_home_score = 0
        self.internal_prev_away_score = 0
        self.internal_prev_game_id_for_score_tracking = None
        self.internal_prev_last_play = "N/A"
        self.internal_prev_last_event_sig = None
        self.internal_prev_last_event_id = None
        self.internal_win_fired_game_id = None
        self.internal_start_fired_game_id = None
        self.player_team_map.clear()
        self._clear_pending_events()
        self.create_task(self._async_initial_state_load())

        lights_status = "ENABLED" if self.lights_enabled_boolean_for_preset_notif and self.get_state(self.lights_enabled_boolean_for_preset_notif) == "on" else "DISABLED"
        horn_status = "ENABLED" if self.horn_enabled_boolean_for_preset_notif and self.get_state(self.horn_enabled_boolean_for_preset_notif) == "on" else "DISABLED"
        tts_status = "ENABLED" if self.tts_enabled_boolean_for_preset_notif and self.get_state(self.tts_enabled_boolean_for_preset_notif) == "on" else "DISABLED"

        if self.pushover_main_notifier:
            tmpl = {
                "new_preset": new_state_val,
                "lights_status": lights_status,
                "horn_status": horn_status,
                "tts_status": tts_status
            }
            title = self._render_template("preset_change_title", "🏒 NHL Tracking Update", **tmpl)
            message = self._render_template(
                "preset_change_body",
                "Changed to: <b>{new_preset}</b>.<br>Goal Lights: <b>{lights_status}</b> | Goal Horn: <b>{horn_status}</b> | TTS: <b>{tts_status}</b>",
                **tmpl
            )
            self.send_notification(self.pushover_main_notifier, title, message, {"html": 1, "priority": -1, "sound": "pushover"})

    # -----------------------------------------------------------------------
    # Goal/penalty detail extraction helpers
    # -----------------------------------------------------------------------

    def _match_goal_detail(
        self,
        feed: Any,
        target_team_abbr: Optional[str],
        score_str: Optional[str],
        scorer_name: Optional[str],
        goal_time: Optional[str]
    ) -> Dict[str, Any]:
        if not feed or not isinstance(feed, list):
            return {}
        target_team_abbr = (target_team_abbr or "").upper()
        normalized_scorer = self._normalize_name(scorer_name)
        normalized_time = (goal_time or "").strip()
        score_str = str(score_str or "").strip()

        for entry in reversed(feed):
            if not isinstance(entry, dict):
                continue
            entry_team = (entry.get("team_abbr") or entry.get("team") or "").upper()
            if target_team_abbr and entry_team and entry_team != target_team_abbr:
                continue
            entry_score = str(entry.get("score_str") or entry.get("score") or "").strip()
            if score_str and entry_score and entry_score != score_str:
                continue
            entry_time = str(entry.get("time") or entry.get("time_in_period") or entry.get("timeInPeriod") or "").strip()
            if normalized_time and entry_time and entry_time != normalized_time:
                continue
            entry_scorer = self._cleanup_player_display(entry.get("scorer") or entry.get("goal_scorer"))
            if normalized_scorer and self._normalize_name(entry_scorer) != normalized_scorer:
                continue
            assists_raw = entry.get("assists")
            assists = []
            if isinstance(assists_raw, list):
                assists = [self._cleanup_player_display(a) for a in assists_raw if a]
            elif isinstance(assists_raw, str):
                assists = [self._cleanup_player_display(a) for a in assists_raw.split(",") if a]
            detail = {
                "scorer": entry_scorer or scorer_name,
                "assists": assists,
                "shot_type": entry.get("shot_type") or entry.get("shotType"),
                "strength": entry.get("strength") or entry.get("goal_strength"),
                "time": entry_time or normalized_time,
                "period_ord": entry.get("period_ord") or entry.get("periodOrd"),
                "score_str": entry_score or score_str,
                "team_abbr": entry_team or target_team_abbr
            }
            return detail
        return {}

    def _match_penalty_detail(
        self,
        feed: Any,
        team_abbr: Optional[str],
        penalty_name: Optional[str],
        player_name: Optional[str],
        penalty_time: Optional[str],
        period_ord: Optional[str]
    ) -> Dict[str, Any]:
        if not feed or not isinstance(feed, list):
            return {}
        team_abbr = (team_abbr or "").upper()
        normalized_player = self._normalize_name(player_name)
        normalized_penalty = self._normalize_name(penalty_name)
        normalized_time = (penalty_time or "").strip()
        normalized_period = (period_ord or "").strip()

        for entry in reversed(feed):
            if not isinstance(entry, dict):
                continue
            entry_team = (entry.get("team_abbr") or entry.get("team") or "").upper()
            if team_abbr and entry_team and entry_team != team_abbr:
                continue
            entry_penalty = self._cleanup_player_display(entry.get("name") or entry.get("penalty_name") or entry.get("penalty"))
            if normalized_penalty and self._normalize_name(entry_penalty) != normalized_penalty:
                continue
            entry_player = self._cleanup_player_display(entry.get("who") or entry.get("player") or entry.get("player_name"))
            if normalized_player and self._normalize_name(entry_player) != normalized_player:
                continue
            entry_time = str(entry.get("time") or entry.get("time_in_period") or entry.get("timeInPeriod") or "").strip()
            if normalized_time and entry_time and entry_time != normalized_time:
                continue
            entry_period = str(entry.get("period_ord") or entry.get("periodOrd") or entry.get("period") or "").strip()
            if normalized_period and entry_period and entry_period != normalized_period:
                continue
            minutes = entry.get("minutes") or entry.get("penalty_minutes") or entry.get("duration") or entry.get("mins")
            try:
                minutes = int(minutes)
            except Exception:
                minutes = minutes
            detail = {
                "who": entry_player or player_name,
                "name": entry_penalty or penalty_name,
                "minutes": minutes,
                "drawn_by": self._cleanup_player_display(entry.get("drawn_by") or entry.get("drawnBy") or entry.get("drawn_by_player")),
                "served_by": self._cleanup_player_display(entry.get("served_by") or entry.get("servedBy")),
                "result": entry.get("result"),
                "team_abbr": entry_team or team_abbr,
                "time": entry_time or normalized_time,
                "period_ord": entry_period or normalized_period
            }
            return detail
        return {}

    # -----------------------------------------------------------------------
    # NHL API sensor bridge
    # -----------------------------------------------------------------------

    def nhl_api_sensor_change_callback(self, entity, attribute, old, new, kwargs):
        if not new or not isinstance(new, dict):
            return
        state = new.get("state")
        if state in (None, "unknown", "unavailable"):
            return
        attrs = new.get("attributes", {}) or {}
        if not isinstance(attrs, dict):
            return

        selected_team_abbrev = self._get_selected_team_abbrev()
        self.log_message(
            f"NHL API sensor update: state={state}; preset_abbr={selected_team_abbrev}; game_id={attrs.get('game_id')}",
            level="DEBUG"
        )
        if not selected_team_abbrev or selected_team_abbrev == "NONE":
            return

        context = self._build_nhl_api_context(attrs, selected_team_abbrev)
        if not context:
            self.log_message("Context build failed (team mismatch).", level="DEBUG")
            return

        game_id = context.get("game_id")
        if game_id and game_id != self.internal_prev_game_id_for_score_tracking:
            self.log_message(f"Detected new game_id {game_id}; resetting trackers.", level="DEBUG")
            self._clear_pending_events()
            self.internal_prev_game_id_for_score_tracking = game_id
            self.internal_prev_last_event_id = None
            self.internal_prev_last_event_sig = None
            self.internal_win_fired_game_id = None
            self.internal_start_fired_game_id = None
            self.player_team_map.clear()

        previous_state = old.get("state") if isinstance(old, dict) else None
        if state == "LIVE" and previous_state not in ("LIVE", "CRIT"):
            self._fire_puck_drop(context, attrs)

        self.internal_prev_home_score = context["home_score"]
        self.internal_prev_away_score = context["away_score"]

        goal_event_id = attrs.get("goal_event_id")
        if goal_event_id:
            event_id = str(goal_event_id)
            if event_id in self.nhl_api_goal_ids_processed:
                self.log_message(f"Goal event {event_id} already processed; skipping.", level="DEBUG")
            else:
                is_my_goal = self._determine_goal_side(attrs, context)
                payload = self._build_nhl_api_goal_payload(attrs, context, is_my_goal)
                if payload:
                    score_sig = (context["home_score"], context["away_score"])
                    old_timer = self.pending_sb_goal_timers.pop(score_sig, None)
                    if old_timer:
                        try:
                            if self.timer_running(old_timer):
                                self.cancel_timer(old_timer)
                                self.log_message(f"Cancelled scoreboard fallback for score {score_sig} because API goal arrived.", level="DEBUG")
                        except Exception:
                            pass
                    self.sb_last_fired_home_away = score_sig
                    self.sb_goal_suppress_until_ts = time.time() + self.suppress_after_scoreboard_goal_seconds

                    total_delay = max(0.1, float(self.coalesce_goal_seconds) + self._get_broadcast_delay())
                    self._schedule_event_coalesced(event_id=event_id, event_type="goal", payload=payload, delay=total_delay)
                    self.nhl_api_goal_ids_processed.add(event_id)
                else:
                    self.log_message("Goal payload could not be built; skipping.", level="WARNING")

        if state in ["FINAL", "OFF"]:
            self._process_nhl_api_win(attrs, context)

    def _determine_goal_side(self, attrs: Dict[str, Any], context: Dict[str, Any]) -> bool:
        goal_team = (attrs.get("goal_team_abbrev") or attrs.get("goal_team_abbr") or "").upper()
        tracked_flag = attrs.get("goal_tracked_team")
        my_abbr = context["my_team_abbr"]
        opp_abbr = context["opp_team_abbr"]

        if goal_team:
            if goal_team == my_abbr:
                return True
            if goal_team == opp_abbr:
                return False

        if isinstance(tracked_flag, bool):
            return tracked_flag

        prev_home = self.internal_prev_home_score
        prev_away = self.internal_prev_away_score
        cur_home = context["home_score"]
        cur_away = context["away_score"]

        if my_abbr == context["home_abbr"]:
            if cur_home > prev_home:
                return True
            if cur_away > prev_away:
                return False
        else:
            if cur_away > prev_away:
                return True
            if cur_home > prev_home:
                return False

        self.log_message("Goal side undetermined; defaulting to opponent.", level="INFO")
        return False

    def _fire_puck_drop(self, context: Dict[str, Any], api_attrs: Dict[str, Any]) -> None:
        game_id = context.get("game_id")
        if game_id and self.internal_start_fired_game_id == game_id:
            return

        event = {
            "team_name": API_TO_STANDARD_TEAM_NAME_MAP.get(context["my_team_full"], context["my_team_full"]),
            "my_team_name": context["my_team_full"],
            "my_team_abbr": context["my_team_abbr"],
            "opp_team_name": context["opp_team_full"],
            "opp_team_abbr": context["opp_team_abbr"],
            "my_team_score": context["home_score"] if context["is_home"] else context["away_score"],
            "opp_team_score": context["away_score"] if context["is_home"] else context["home_score"],
            "period": "Pregame",
            "time_remaining": api_attrs.get("time_remaining") or "--:--",
            "source": f"nhl_notifications_app_v{self.APP_VERSION} (PUCK_DROP)"
        }

        self.log_message("Puck drop detected; firing goal_event for celebration.", level="DEBUG")
        self.fire_event(self.goal_event_to_fire, **event)

        if self.pushover_main_notifier:
            tmpl = {
                "my_team_full": context["my_team_full"],
                "opp_team_full": context["opp_team_full"]
            }
            title = self._render_template("live_game_title", "🏒 Game is LIVE!", **tmpl)
            message = self._render_template("live_game_body", "{my_team_full} vs {opp_team_full} is underway.", **tmpl)
            data = {"priority": 0, "sound": self._generate_pushover_sound_name(context["my_team_full"]), "html": 1}
            self.send_notification(self.pushover_main_notifier, title, message, data)

        self.internal_start_fired_game_id = game_id

    def _build_nhl_api_context(self, attrs: Dict[str, Any], selected_team_abbrev: str) -> Optional[Dict[str, Any]]:
        home_abbr = (attrs.get("home_abbr") or "").upper()
        away_abbr = (attrs.get("away_abbr") or "").upper()
        sensor_abbr = (attrs.get("team_abbrev") or "").upper()

        target_abbr = selected_team_abbrev
        if target_abbr not in (home_abbr, away_abbr):
            if sensor_abbr in (home_abbr, away_abbr):
                target_abbr = sensor_abbr
            else:
                return None

        is_home = target_abbr == home_abbr
        my_team_full = attrs.get("my_team_name") or (attrs.get("home_name") if is_home else attrs.get("away_name")) \
            or NHL_TEAM_ABBREV_TO_FULL_NAME_MAP.get(target_abbr, target_abbr)
        opp_team_full = attrs.get("opponent_name") or (attrs.get("away_name") if is_home else attrs.get("home_name")) or "Opponent"

        home_logo = attrs.get("home_logo_dark") or attrs.get("home_logo")
        away_logo = attrs.get("away_logo_dark") or attrs.get("away_logo")
        my_logo = home_logo if is_home else away_logo
        opp_logo = away_logo if is_home else home_logo

        try:
            home_score = int(attrs.get("home_score", 0))
        except Exception:
            home_score = 0
        try:
            away_score = int(attrs.get("away_score", 0))
        except Exception:
            away_score = 0

        return {
            "my_team_abbr": target_abbr,
            "my_team_full": my_team_full,
            "opp_team_abbr": away_abbr if is_home else home_abbr,
            "opp_team_full": opp_team_full,
            "is_home": is_home,
            "home_abbr": home_abbr,
            "away_abbr": away_abbr,
            "home_logo": home_logo,
            "away_logo": away_logo,
            "my_logo": my_logo,
            "opp_logo": opp_logo,
            "home_score": home_score,
            "away_score": away_score,
            "game_url": attrs.get("game_url") or (f"https://www.nhl.com/gamecenter/{attrs.get('game_id')}" if attrs.get("game_id") else None),
            "game_id": attrs.get("game_id")
        }

    def _build_nhl_api_goal_payload(self, attrs: Dict[str, Any], context: Dict[str, Any], is_my_event: bool) -> Optional[Dict[str, Any]]:
        scorer = attrs.get("scoring_player_name") or attrs.get("scoring_player") or "a player"
        assist1 = attrs.get("assist1_player_name")
        assist2 = attrs.get("assist2_player_name")
        assists = [self._cleanup_player_display(a) for a in [assist1, assist2] if a]
        if isinstance(attrs.get("assists"), list) and not assists:
            assists = [self._cleanup_player_display(a) for a in attrs.get("assists") if a]

        period_ord = attrs.get("current_period") or attrs.get("period") or ""
        time_remaining = attrs.get("time_remaining") or attrs.get("goal_time") or "--:--"
        strength = attrs.get("goal_type")
        shot_type = attrs.get("shot_type")

        if context["is_home"]:
            my_score = context["home_score"]
            opp_score = context["away_score"]
        else:
            my_score = context["away_score"]
            opp_score = context["home_score"]

        goal_team_abbr = context["my_team_abbr"] if is_my_event else context["opp_team_abbr"]
        score_str = f"{context['home_score']}-{context['away_score']}"
        goal_feed = attrs.get("scoring_detailed") or attrs.get("scoring_detail") or []
        goal_detail = self._match_goal_detail(goal_feed, goal_team_abbr, score_str, scorer, attrs.get("goal_time") or time_remaining)

        last_play = attrs.get("last_goal_description")
        if not last_play:
            last_play = f"Goal: {self._cleanup_player_display(scorer)} ({goal_team_abbr}) at {time_remaining} of {period_ord or 'current period'} (NHL API)"

        raw_last_event = {
            "type": "goal",
            "team": goal_team_abbr,
            "scorer": scorer,
            "assist1": assist1,
            "assist2": assist2,
            "strength": strength,
            "shotType": shot_type,
            "timeInPeriod": attrs.get("goal_time") or time_remaining,
            "periodOrd": period_ord
        }

        return {
            "is_my_event": is_my_event,
            "my_team_abbr": context["my_team_abbr"],
            "my_team_full": context["my_team_full"],
            "opp_team_abbr": context["opp_team_abbr"],
            "opp_team_full": context["opp_team_full"],
            "home_abbr": context["home_abbr"],
            "away_abbr": context["away_abbr"],
            "home_logo": context["home_logo"],
            "away_logo": context["away_logo"],
            "period_ord": period_ord,
            "time_remaining": time_remaining,
            "game_url": context["game_url"],
            "my_score": my_score,
            "opp_score": opp_score,
            "last_play": last_play,
            "raw_last_event": raw_last_event,
            "assists": assists,
            "score_str": score_str,
            "goal_team_abbr": goal_team_abbr,
            "goal_detail": goal_detail,
            "scoring_feed": goal_feed
        }

    def _process_nhl_api_win(self, attrs: Dict[str, Any], context: Dict[str, Any]) -> None:
        game_id = context.get("game_id")
        if not game_id:
            return
        my_score = context["home_score"] if context["is_home"] else context["away_score"]
        opp_score = context["away_score"] if context["is_home"] else context["home_score"]
        if my_score <= opp_score:
            return
        if self.internal_win_fired_game_id == game_id:
            return
        if self.celebrate_win_only_if_home and not context["is_home"]:
            return

        delay = self._get_broadcast_delay()

        def _fire_win(_k=None):
            tmpl = {
                "my_team_name": context["my_team_full"],
                "opp_team_name": context["opp_team_full"],
                "my_team_score": my_score,
                "opp_team_score": opp_score
            }

            if self.pushover_main_notifier:
                title = self._render_template("win_title", "🏒 Final Score", **tmpl)
                body = self._render_template(
                    "win_body",
                    "<b>{my_team_name}</b> defeat <b>{opp_team_name}</b>, {my_team_score}-{opp_team_score}.",
                    **tmpl
                )
                self.send_notification(self.pushover_main_notifier, title, body, {"html": 1, "priority": 0, "sound": "pushover"})

            event = {
                "team_name": API_TO_STANDARD_TEAM_NAME_MAP.get(context["my_team_full"], context["my_team_full"]),
                "my_team_name": context["my_team_full"],
                "my_team_abbr": context["my_team_abbr"],
                "opp_team_name": context["opp_team_full"],
                "opp_team_abbr": context["opp_team_abbr"],
                "my_team_score": my_score,
                "opp_team_score": opp_score,
                "is_home": context["is_home"],
                "tts_phrase": self._render_template("win_tts", "The {my_team_name} win {my_team_score} to {opp_team_score}.", **tmpl),
                "source": f"nhl_notifications_app_v{self.APP_VERSION} (NHL_API_WIN)"
            }
            self.fire_event(self.team_win_event_to_fire, **event)
            self.internal_win_fired_game_id = game_id

        self.run_in(_fire_win, max(0.1, delay))

    # -----------------------------------------------------------------------
    # Dashboard sensor handling / scoreboard fallbacks
    # -----------------------------------------------------------------------

    def dashboard_sensor_change_callback(self, entity, attribute, old, new, kwargs):
        is_test = kwargs.get("is_test_trigger", False)
        if not new or not isinstance(new, dict) or new.get("state") == "unavailable":
            return

        new_attrs = new.get("attributes", {})
        old_attrs = old.get("attributes", {}) if isinstance(old, dict) else {}

        current_game_id = new_attrs.get("game_id")
        if current_game_id and current_game_id != self.internal_prev_game_id_for_score_tracking:
            self.log_message(f"Dashboard: new game_id {current_game_id}; resetting trackers.", level="DEBUG")
            self._clear_pending_events()
            self.internal_prev_home_score = new_attrs.get("home_score", 0)
            self.internal_prev_away_score = new_attrs.get("away_score", 0)
            self.internal_prev_game_id_for_score_tracking = current_game_id
            self.internal_prev_last_play = new_attrs.get("last_play", "N/A")
            self.internal_prev_last_event_sig = None
            self.internal_prev_last_event_id = None
            self.internal_win_fired_game_id = None
            self.internal_start_fired_game_id = None
            self.player_team_map.clear()
        elif not current_game_id and not is_test:
            self._clear_pending_events()
            self.internal_prev_home_score = 0
            self.internal_prev_away_score = 0
            self.internal_prev_game_id_for_score_tracking = None
            self.internal_prev_last_play = "N/A"
            self.internal_prev_last_event_sig = None
            self.internal_prev_last_event_id = None
            self.internal_win_fired_game_id = None
            self.internal_start_fired_game_id = None
            self.player_team_map.clear()

        selected_abbr = self._get_selected_team_abbrev()
        home_abbr = (new_attrs.get("home_abbr") or "HME").upper()
        away_abbr = (new_attrs.get("away_abbr") or "AWY").upper()

        if selected_abbr == home_abbr:
            my_team_full = new_attrs.get("home_name", "Home")
            opp_team_full = new_attrs.get("away_name", "Away")
            my_team_abbr = home_abbr
            opp_team_abbr = away_abbr
            my_logo = new_attrs.get("home_logo", "")
            opp_logo = new_attrs.get("away_logo", "")
            my_score_new = int(new_attrs.get("home_score", 0))
            opp_score_new = int(new_attrs.get("away_score", 0))
            my_score_prev = self.internal_prev_home_score
            opp_score_prev = self.internal_prev_away_score
            is_home = True
        elif selected_abbr == away_abbr:
            my_team_full = new_attrs.get("away_name", "Away")
            opp_team_full = new_attrs.get("home_name", "Home")
            my_team_abbr = away_abbr
            opp_team_abbr = home_abbr
            my_logo = new_attrs.get("away_logo", "")
            opp_logo = new_attrs.get("home_logo", "")
            my_score_new = int(new_attrs.get("away_score", 0))
            opp_score_new = int(new_attrs.get("home_score", 0))
            my_score_prev = self.internal_prev_away_score
            opp_score_prev = self.internal_prev_home_score
            is_home = False
        else:
            return

        my_scored = my_score_new > my_score_prev
        opp_scored = opp_score_new > opp_score_prev

        new_state = new.get("state", "").upper()
        last_event = new_attrs.get("last_event", {}) or {}
        last_event_id = new_attrs.get("last_event_id")

        scheduled_by_event = False
        if new_state in ["LIVE", "CRIT"] and last_event and last_event_id and (last_event_id not in self.fired_event_ids):
            evt_type = (last_event.get("type") or "").lower()
            evt_team = (last_event.get("team") or "").upper()
            penalties_feed = new_attrs.get("penalties_detailed")
            scoring_feed = new_attrs.get("scoring_detailed")
            score_str = f"{new_attrs.get('home_score', 0)}-{new_attrs.get('away_score', 0)}"

            if evt_type in ("goal", "penalty"):
                if evt_type == "goal":
                    pending = self.pending_sb_goal_timers.pop((new_attrs.get("home_score", 0), new_attrs.get("away_score", 0)), None)
                    if pending:
                        try:
                            if self.timer_running(pending):
                                self.cancel_timer(pending)
                                self.log_message(f"Dashboard: cancelled fallback due to event {last_event_id}.", level="DEBUG")
                        except Exception:
                            pass

                    is_my_event = (evt_team == my_team_abbr) if evt_team else my_scored
                    goal_team_abbr = evt_team or (my_team_abbr if is_my_event else opp_team_abbr)
                    goal_detail = self._match_goal_detail(
                        scoring_feed,
                        goal_team_abbr,
                        score_str,
                        last_event.get("scorer"),
                        last_event.get("timeInPeriod")
                    )
                    delay = max(0.1, float(self.coalesce_goal_seconds) + self._get_broadcast_delay())
                    self._schedule_event_coalesced(
                        event_id=str(last_event_id),
                        event_type="goal",
                        payload={
                            "is_my_event": is_my_event,
                            "my_team_abbr": my_team_abbr,
                            "my_team_full": my_team_full,
                            "opp_team_abbr": opp_team_abbr,
                            "opp_team_full": opp_team_full,
                            "home_abbr": home_abbr,
                            "away_abbr": away_abbr,
                            "home_logo": new_attrs.get("home_logo", ""),
                            "away_logo": new_attrs.get("away_logo", ""),
                            "period_ord": new_attrs.get("period_ord", ""),
                            "time_remaining": new_attrs.get("time_remaining", ""),
                            "game_url": new_attrs.get("game_url", ""),
                            "my_score": my_score_new,
                            "opp_score": opp_score_new,
                            "last_play": new_attrs.get("last_play", "N/A"),
                            "raw_last_event": last_event,
                            "score_str": score_str,
                            "goal_team_abbr": goal_team_abbr,
                            "goal_detail": goal_detail,
                            "scoring_feed": scoring_feed
                        },
                        delay=delay
                    )
                    scheduled_by_event = True
                else:
                    penalty_detail = self._match_penalty_detail(
                        penalties_feed,
                        evt_team,
                        last_event.get("penaltyName") or last_event.get("penalty_name") or last_event.get("descKey"),
                        last_event.get("penalty_committed_by") or last_event.get("playerName"),
                        last_event.get("timeInPeriod"),
                        last_event.get("periodOrd")
                    )
                    delay = max(0.1, float(self.coalesce_penalty_seconds) + self._get_broadcast_delay())
                    self._schedule_event_coalesced(
                        event_id=str(last_event_id),
                        event_type="penalty",
                        payload={
                            "my_team_abbr": my_team_abbr,
                            "opp_team_abbr": opp_team_abbr,
                            "period_ord": new_attrs.get("period_ord", ""),
                            "time_remaining": new_attrs.get("time_remaining", ""),
                            "raw_last_event": last_event,
                            "penalties_feed": penalties_feed,
                            "penalty_detail": penalty_detail,
                            "scoring_feed": scoring_feed
                        },
                        delay=delay
                    )
                    scheduled_by_event = True

        if not scheduled_by_event:
            scoring_feed = new_attrs.get("scoring_detailed")
            if my_scored:
                delay = self._get_broadcast_delay()
                score_sig = (new_attrs.get("home_score", 0), new_attrs.get("away_score", 0))
                score_str = f"{score_sig[0]}-{score_sig[1]}"
                goal_team_abbr = home_abbr if is_home else away_abbr
                goal_detail = self._match_goal_detail(scoring_feed, goal_team_abbr, score_str, None, None)
                handle = self.run_in(
                    self._scoreboard_goal_fire_wrapper,
                    max(0.1, delay),
                    kind="my",
                    my_team_full=my_team_full,
                    my_abbr=my_team_abbr,
                    my_logo=my_logo,
                    opp_team_full=opp_team_full,
                    opp_abbr=opp_team_abbr,
                    opp_logo=opp_logo,
                    my_score=my_score_new,
                    opp_score=opp_score_new,
                    period_ord=new_attrs.get("period_ord", ""),
                    time_remaining=new_attrs.get("time_remaining", ""),
                    last_play=new_attrs.get("last_play", "N/A"),
                    game_url=new_attrs.get("game_url", ""),
                    score_sig=score_sig,
                    scoring_feed=scoring_feed,
                    goal_detail=goal_detail,
                    goal_team_abbr=goal_team_abbr,
                    score_str=score_str
                )
                self.pending_sb_goal_timers[score_sig] = handle
            elif opp_scored and self.opponent_goal_event_to_fire:
                delay = self._get_broadcast_delay()
                score_sig = (new_attrs.get("home_score", 0), new_attrs.get("away_score", 0))
                score_str = f"{score_sig[0]}-{score_sig[1]}"
                goal_team_abbr = opp_team_abbr
                goal_detail = self._match_goal_detail(scoring_feed, goal_team_abbr, score_str, None, None)
                handle = self.run_in(
                    self._scoreboard_goal_fire_wrapper,
                    max(0.1, delay),
                    kind="opp",
                    opp_team_full=opp_team_full,
                    opp_logo=opp_logo,
                    my_abbr=my_team_abbr,
                    opp_abbr=opp_team_abbr,
                    my_logo=my_logo,
                    my_score=my_score_new,
                    opp_score=opp_score_new,
                    period_ord=new_attrs.get("period_ord", ""),
                    time_remaining=new_attrs.get("time_remaining", ""),
                    last_play=new_attrs.get("last_play", "N/A"),
                    game_url=new_attrs.get("game_url", ""),
                    score_sig=score_sig,
                    scoring_feed=scoring_feed,
                    goal_detail=goal_detail,
                    goal_team_abbr=goal_team_abbr,
                    score_str=score_str
                )
                self.pending_sb_goal_timers[score_sig] = handle

        new_game_state_api = new_attrs.get("game_state_api", "UNKNOWN")
        my_team_won = (new_game_state_api in ["FINAL", "OFF"]) and (my_score_new > opp_score_new)
        if my_team_won and current_game_id:
            if (self.internal_win_fired_game_id != current_game_id) and (not self.celebrate_win_only_if_home or is_home):
                delay = self._get_broadcast_delay()

                def _fire_win(_k=None):
                    event = {
                        "team_name": API_TO_STANDARD_TEAM_NAME_MAP.get(my_team_full, my_team_full),
                        "my_team_name": my_team_full,
                        "my_team_abbr": my_team_abbr,
                        "opp_team_name": opp_team_full,
                        "opp_team_abbr": opp_team_abbr,
                        "my_team_score": my_score_new,
                        "opp_team_score": opp_score_new,
                        "is_home": is_home,
                        "source": f"nhl_notifications_app_v{self.APP_VERSION} (WIN)"
                    }
                    self.fire_event(self.team_win_event_to_fire, **event)
                    self.internal_win_fired_game_id = current_game_id

                self.run_in(_fire_win, max(0.1, delay))

        self.internal_prev_last_play = new_attrs.get("last_play", "N/A")
        if current_game_id == self.internal_prev_game_id_for_score_tracking:
            if my_scored or opp_scored:
                self.internal_prev_home_score = int(new_attrs.get("home_score", 0))
                self.internal_prev_away_score = int(new_attrs.get("away_score", 0))

    # -----------------------------------------------------------------------
    # Coalescing & event firing
    # -----------------------------------------------------------------------

    def _schedule_event_coalesced(self, event_id: str, event_type: str, payload: Dict[str, Any], delay: float):
        if event_id in self.fired_event_ids:
            self.log_message(f"Event {event_id} already fired; skipping schedule.", level="DEBUG")
            return
        self.pending_event_payloads[event_id] = payload
        if event_id not in self.pending_event_timers:
            handle = self.run_in(self._coalesced_fire_callback, max(0.1, float(delay)), event_id=event_id, event_type=event_type)
            self.pending_event_timers[event_id] = handle
            self.log_message(f"Scheduled coalesced {event_type} for event_id {event_id} in {delay:.2f}s.", level="DEBUG")

    def _coalesced_fire_callback(self, kwargs: Dict[str, Any]):
        event_id = str(kwargs.get("event_id"))
        event_type = str(kwargs.get("event_type") or "").lower()
        payload = self.pending_event_payloads.get(event_id, {}) or {}

        dash = self.get_state(self.dashboard_sensor_entity_id, attribute="all") or {}
        attrs = dash.get("attributes", {}) if isinstance(dash, dict) else {}
        current_event_id = str(attrs.get("last_event_id") or "")
        current_last_event = attrs.get("last_event", {}) or {}

        if current_event_id == event_id and isinstance(current_last_event, dict):
            payload["raw_last_event"] = current_last_event
            payload["period_ord"] = attrs.get("period_ord") or payload.get("period_ord")
            payload["time_remaining"] = attrs.get("time_remaining") or payload.get("time_remaining")
            if "my_team_abbr" in payload and "home_abbr" in payload and "away_abbr" in payload:
                try:
                    if payload["my_team_abbr"] == payload["home_abbr"]:
                        payload["my_score"] = int(attrs.get("home_score", payload.get("my_score")))
                        payload["opp_score"] = int(attrs.get("away_score", payload.get("opp_score")))
                    else:
                        payload["my_score"] = int(attrs.get("away_score", payload.get("my_score")))
                        payload["opp_score"] = int(attrs.get("home_score", payload.get("opp_score")))
                except Exception:
                    pass

        if event_type == "goal":
            self._coalesced_fire_goal(event_id, payload)
        elif event_type == "penalty":
            self._coalesced_fire_penalty(event_id, payload)

        self.fired_event_ids.add(event_id)
        handle = self.pending_event_timers.pop(event_id, None)
        if handle:
            try:
                if self.timer_running(handle):
                    self.cancel_timer(handle)
            except Exception:
                pass
        self.pending_event_payloads.pop(event_id, None)
        self.internal_prev_last_event_id = event_id

    def _coalesced_fire_goal(self, event_id: str, payload: Dict[str, Any]):
        try:
            home_abbr = payload.get("home_abbr")
            my_abbr = payload.get("my_team_abbr")
            my_score = int(payload.get("my_score"))
            opp_score = int(payload.get("opp_score"))
            if home_abbr and my_abbr:
                home_score_now, away_score_now = (my_score, opp_score) if my_abbr == home_abbr else (opp_score, my_score)
                if (time.time() < self.sb_goal_suppress_until_ts) and (self.sb_last_fired_home_away == (home_score_now, away_score_now)):
                    self.log_message("Skipping coalesced goal: already handled via scoreboard.", level="DEBUG")
                    return
        except Exception:
            pass

        last_evt = payload.get("raw_last_event", {}) or {}
        detail = payload.get("goal_detail") or {}
        if not detail:
            detail = self._match_goal_detail(
                payload.get("scoring_feed"),
                payload.get("goal_team_abbr"),
                payload.get("score_str"),
                last_evt.get("scorer"),
                last_evt.get("timeInPeriod")
            )

        scorer = self._cleanup_player_display(detail.get("scorer") or last_evt.get("scorer") or "a player")
        assists = detail.get("assists")
        if not assists:
            assists = [self._cleanup_player_display(a) for a in payload.get("assists", []) if a]
        strength = detail.get("strength") or last_evt.get("strength") or ""
        shot_type = detail.get("shot_type") or last_evt.get("shotType") or ""
        period_ord = detail.get("period_ord") or payload.get("period_ord") or last_evt.get("periodOrd") or ""
        when = detail.get("time") or last_evt.get("timeInPeriod") or payload.get("time_remaining") or ""

        target_team_abbr = payload.get("goal_team_abbr") if payload.get("is_my_event") else payload.get("opp_team_abbr")
        self._remember_players_for_team([scorer] + assists, target_team_abbr)

        context, default_title, default_body, default_tts = self._build_goal_texts(
            is_my_goal=payload["is_my_event"],
            scorer=scorer,
            assists=assists,
            shot_type=shot_type,
            strength=strength,
            period_ord=period_ord,
            when=when,
            my_team_name=payload["my_team_full"],
            my_team_abbr=payload["my_team_abbr"],
            opp_team_name=payload["opp_team_full"],
            opp_team_abbr=payload["opp_team_abbr"],
            my_score=payload["my_score"],
            opp_score=payload["opp_score"],
            game_url=payload.get("game_url"),
            source=f"nhl_notifications_app_v{self.APP_VERSION} (COALESCED)"
        )

        if payload.get("home_abbr"):
            if payload["my_team_abbr"] == payload["home_abbr"]:
                context["my_logo"] = payload.get("home_logo")
                context["opp_logo"] = payload.get("away_logo")
            else:
                context["my_logo"] = payload.get("away_logo")
                context["opp_logo"] = payload.get("home_logo")

        tts_phrase = self._dispatch_goal_notifications(context, default_title, default_body, default_tts)
        context["tts_phrase"] = tts_phrase
        self._emit_goal_events(context, scorer, assists, shot_type, strength, period_ord, when)

    def _coalesced_fire_penalty(self, event_id: str, payload: Dict[str, Any]):
        try:
            last_evt = payload.get("raw_last_event", {}) or {}
            detail = payload.get("penalty_detail") or {}
            if not detail:
                detail = self._match_penalty_detail(
                    payload.get("penalties_feed"),
                    last_evt.get("team"),
                    last_evt.get("penalty_name") or last_evt.get("penaltyName") or last_evt.get("descKey"),
                    last_evt.get("penalty_committed_by") or last_evt.get("playerName"),
                    last_evt.get("timeInPeriod"),
                    last_evt.get("periodOrd")
                )

            who = self._cleanup_player_display(detail.get("who") or last_evt.get("penalty_committed_by") or last_evt.get("playerName") or "Unknown Player")
            penalty_name = detail.get("name") or last_evt.get("penalty_name") or last_evt.get("penaltyName") or detail.get("penalty") or "Penalty"
            minutes = detail.get("minutes") or last_evt.get("penalty_minutes") or last_evt.get("duration")
            try:
                minutes_value = int(minutes)
                minutes_text = f"{minutes_value} minutes"
            except Exception:
                minutes_value = minutes
                minutes_text = str(minutes) if minutes else "Penalty"
            drawn_by = self._cleanup_player_display(detail.get("drawn_by") or last_evt.get("drawn_by_player") or last_evt.get("drawnByPlayerName") or "")
            served_by = self._cleanup_player_display(detail.get("served_by") or "")
            result = detail.get("result") or ""
            when_evt = detail.get("time") or last_evt.get("timeInPeriod") or payload.get("time_remaining") or ""
            period_ord_evt = detail.get("period_ord") or last_evt.get("periodOrd") or payload.get("period_ord") or ""
            team_evt = (detail.get("team_abbr") or last_evt.get("team") or "").upper()
            team_full = NHL_TEAM_ABBREV_TO_FULL_NAME_MAP.get(team_evt, team_evt or "Unknown Team")

            if who:
                normalized_who = self._normalize_name(who)
                if normalized_who and team_evt:
                    self.player_team_map[normalized_who] = team_evt

            time_line = f"{period_ord_evt} • {when_evt}".strip(" •")
            details_lines = []
            if drawn_by:
                details_lines.append(f"Drawn by {drawn_by}")
            if served_by:
                details_lines.append(f"Served by {served_by}")
            if result:
                details_lines.append(f"Result: {result}")
            details_block = "".join(f"<br>{line}" for line in details_lines)

            context = {
                "player": who,
                "penalty_name": penalty_name,
                "minutes": minutes_value,
                "minutes_text": minutes_text,
                "drawn_by": drawn_by,
                "served_by": served_by,
                "result": result,
                "time_line": time_line,
                "details_block": details_block,
                "team_full": team_full,
                "team_abbr": team_evt,
                "source": f"nhl_notifications_app_v{self.APP_VERSION} (COALESCED)"
            }

            title = self._render_template("penalty_title", "🚨 Penalty Called", **context)
            body = self._render_template(
                "penalty_body",
                "<b>{player}</b> • {minutes_text} for <b>{penalty_name}</b>{details_block}<br>{time_line}",
                **context
            )
            penalty_tts = self._render_template(
                "penalty_tts",
                "Penalty called on {player} for {penalty_name}, {minutes_text}.",
                **context
            )
            context["tts_phrase"] = penalty_tts

            self.fire_event(
                "nhl_penalty_event",
                team_abbr=team_evt,
                my_team_abbr=payload.get("my_team_abbr"),
                opp_team_abbr=payload.get("opp_team_abbr"),
                period=period_ord_evt,
                time_remaining=when_evt,
                text=body,
                penalty_tts=penalty_tts,
                player=who,
                penalty_name=penalty_name,
                minutes_text=minutes_text,
                drawn_by=drawn_by,
                served_by=served_by,
                result=result,
                source=context["source"]
            )

            if self.pushover_main_notifier:
                self.send_notification(
                    self.pushover_main_notifier,
                    title,
                    body,
                    {"html": 1, "priority": 0, "sound": "siren"}
                )
        except Exception as e:
            self.log_message(f"Error processing penalty event {event_id}: {e}", level="ERROR")

    # -----------------------------------------------------------------------
    # Scoreboard fallback goal firing
    # -----------------------------------------------------------------------

    def _scoreboard_goal_fire_wrapper(self, kwargs: Dict[str, Any]):
        score_sig = kwargs.get("score_sig")
        kind = kwargs.get("kind")
        goal_detail = kwargs.get("goal_detail")
        scoring_feed = kwargs.get("scoring_feed")
        goal_team_abbr = kwargs.get("goal_team_abbr")
        score_str = kwargs.get("score_str")
        if not goal_detail:
            goal_detail = self._match_goal_detail(scoring_feed, goal_team_abbr, score_str, None, None)
        try:
            if kind == "my":
                self._fire_my_goal_immediate(
                    kwargs.get("my_team_full"),
                    kwargs.get("my_abbr"),
                    kwargs.get("my_logo"),
                    kwargs.get("opp_team_full"),
                    kwargs.get("opp_abbr"),
                    kwargs.get("opp_logo"),
                    kwargs.get("my_score"),
                    kwargs.get("opp_score"),
                    kwargs.get("period_ord"),
                    kwargs.get("time_remaining"),
                    kwargs.get("last_play"),
                    kwargs.get("game_url"),
                    goal_detail=goal_detail,
                    scoring_feed=scoring_feed,
                    goal_team_abbr=goal_team_abbr,
                    score_str=score_str
                )
            else:
                self._fire_opponent_goal_immediate(
                    kwargs.get("opp_team_full"),
                    kwargs.get("opp_logo"),
                    kwargs.get("my_abbr"),
                    kwargs.get("opp_abbr"),
                    kwargs.get("my_logo"),
                    kwargs.get("my_score"),
                    kwargs.get("opp_score"),
                    kwargs.get("period_ord"),
                    kwargs.get("time_remaining"),
                    kwargs.get("last_play"),
                    kwargs.get("game_url"),
                    goal_detail=goal_detail,
                    scoring_feed=scoring_feed,
                    goal_team_abbr=goal_team_abbr,
                    score_str=score_str
                )

            self.sb_last_fired_home_away = score_sig
            self.sb_goal_suppress_until_ts = time.time() + self.suppress_after_scoreboard_goal_seconds
        finally:
            if score_sig in self.pending_sb_goal_timers:
                self.pending_sb_goal_timers.pop(score_sig, None)

    def _fire_my_goal_immediate(self, my_team_full, my_abbr, my_logo, opp_team_full, opp_abbr,
                                opp_logo, my_score, opp_score, period_ord, time_remaining, last_play, game_url,
                                goal_detail=None, scoring_feed=None, goal_team_abbr=None, score_str=None):
        detail = goal_detail or self._match_goal_detail(scoring_feed, goal_team_abbr, score_str, None, None)
        scorer = self._cleanup_player_display(detail.get("scorer")) if detail else ""
        if not scorer:
            scorer, _ = self._parse_scorer_from_last_play(last_play)
        assists = detail.get("assists") if detail else []
        shot_type = detail.get("shot_type") if detail else None
        strength = detail.get("strength") if detail else None
        period_ord = detail.get("period_ord") or period_ord
        when = detail.get("time") or time_remaining

        self._remember_players_for_team([scorer] + assists, my_abbr)

        context, default_title, default_body, default_tts = self._build_goal_texts(
            is_my_goal=True,
            scorer=scorer,
            assists=assists,
            shot_type=shot_type,
            strength=strength,
            period_ord=period_ord,
            when=when,
            my_team_name=my_team_full,
            my_team_abbr=my_abbr,
            opp_team_name=opp_team_full,
            opp_team_abbr=opp_abbr,
            my_score=my_score,
            opp_score=opp_score,
            game_url=game_url,
            source=f"nhl_notifications_app_v{self.APP_VERSION} (SCOREBOARD)"
        )
        context["my_logo"] = my_logo
        context["opp_logo"] = opp_logo

        tts_phrase = self._dispatch_goal_notifications(context, default_title, default_body, default_tts)
        context["tts_phrase"] = tts_phrase
        self._emit_goal_events(context, scorer, assists, shot_type, strength, period_ord, when)

    def _fire_opponent_goal_immediate(self, opp_team_full, opp_logo, my_abbr, opp_abbr,
                                      my_logo, my_score, opp_score, period_ord, time_remaining, last_play, game_url,
                                      goal_detail=None, scoring_feed=None, goal_team_abbr=None, score_str=None):
        detail = goal_detail or self._match_goal_detail(scoring_feed, goal_team_abbr, score_str, None, None)
        scorer = self._cleanup_player_display(detail.get("scorer")) if detail else ""
        if not scorer:
            scorer, _ = self._parse_scorer_from_last_play(last_play)
        assists = detail.get("assists") if detail else []
        shot_type = detail.get("shot_type") if detail else None
        strength = detail.get("strength") if detail else None
        period_ord = detail.get("period_ord") or period_ord
        when = detail.get("time") or time_remaining

        self._remember_players_for_team([scorer] + assists, opp_abbr)

        tracked_team_name = NHL_TEAM_ABBREV_TO_FULL_NAME_MAP.get(my_abbr, my_abbr)

        context, default_title, default_body, default_tts = self._build_goal_texts(
            is_my_goal=False,
            scorer=scorer,
            assists=assists,
            shot_type=shot_type,
            strength=strength,
            period_ord=period_ord,
            when=when,
            my_team_name=tracked_team_name,
            my_team_abbr=my_abbr,
            opp_team_name=opp_team_full,
            opp_team_abbr=opp_abbr,
            my_score=my_score,
            opp_score=opp_score,
            game_url=game_url,
            source=f"nhl_notifications_app_v{self.APP_VERSION} (SCOREBOARD)"
        )
        context["my_logo"] = my_logo
        context["opp_logo"] = opp_logo

        tts_phrase = self._dispatch_goal_notifications(context, default_title, default_body, default_tts)
        context["tts_phrase"] = tts_phrase
        self._emit_goal_events(context, scorer, assists, shot_type, strength, period_ord, when)

    # -----------------------------------------------------------------------
    # Test helpers
    # -----------------------------------------------------------------------

    def test_notification_callback(self, entity, attribute, old, new, kwargs):
        is_opponent_test = kwargs.get("is_opponent_test", False)

        if not self.dashboard_sensor_entity_id or not self.entity_exists(self.dashboard_sensor_entity_id):
            if self.entity_exists(entity):
                self.turn_off(entity)
            return

        current_dashboard_state = self.get_state(self.dashboard_sensor_entity_id, attribute="all")
        if not current_dashboard_state or not isinstance(current_dashboard_state, dict):
            if self.entity_exists(entity):
                self.turn_off(entity)
            return

        mock_new_state = copy.deepcopy(current_dashboard_state)
        mock_new_attrs = mock_new_state.get("attributes", {}) or {}

        preset = str(self.get_state(self.team_notification_preset_select) or "None")
        api_style_name = PRESET_TO_API_STYLE_NAME_MAP.get(preset, preset)
        my_abbr = NHL_TEAM_NAME_TO_ABBREV_MAP.get(api_style_name)
        if not my_abbr or my_abbr == "NONE":
            if self.entity_exists(entity):
                self.turn_off(entity)
            return

        mock_old_state = copy.deepcopy(current_dashboard_state)

        home_abbr = (mock_new_attrs.get("home_abbr") or "").upper()
        away_abbr = (mock_new_attrs.get("away_abbr") or "").upper()
        if my_abbr not in (home_abbr, away_abbr):
            fallback = ["NYI", "PHI", "OTT", "CBJ", "SEA", "VGK"]
            opp_abbr = next((o for o in fallback if o != my_abbr), "NYI")
            mock_new_attrs.update({
                "game_id": mock_new_attrs.get("game_id") or f"testgame_{datetime.datetime.now().strftime('%H%M%S')}",
                "game_state_api": "LIVE",
                "home_abbr": my_abbr,
                "home_name": NHL_TEAM_ABBREV_TO_FULL_NAME_MAP.get(my_abbr, "Home Team"),
                "away_abbr": opp_abbr,
                "away_name": NHL_TEAM_ABBREV_TO_FULL_NAME_MAP.get(opp_abbr, "Opponent"),
                "period": 1,
                "period_ord": "1st",
                "time_remaining": "19:00",
                "in_intermission": False,
            })
            mock_new_state["state"] = "LIVE"
            mock_new_attrs["home_score"] = int(mock_new_attrs.get("home_score") or 0)
            mock_new_attrs["away_score"] = int(mock_new_attrs.get("away_score") or 0)

        self.internal_prev_game_id_for_score_tracking = mock_new_attrs.get("game_id", f"testgame_{datetime.datetime.now().strftime('%H%M%S')}")
        self.internal_prev_home_score = int(mock_new_attrs.get("home_score", 0))
        self.internal_prev_away_score = int(mock_new_attrs.get("away_score", 0))
        self.internal_prev_last_play = mock_new_attrs.get("last_play", "N/A")
        self.internal_prev_last_event_sig = None
        self.internal_prev_last_event_id = None

        scorer_name = "Test Scorer"
        assist1 = "Test Assist1"
        assist2 = "Test Assist2"
        shot_type = "Wrist"
        strength = "PPG"

        home_abbr = (mock_new_attrs.get("home_abbr") or "").upper()
        away_abbr = (mock_new_attrs.get("away_abbr") or "").upper()

        if is_opponent_test:
            if my_abbr == home_abbr:
                mock_new_attrs["away_score"] = self.internal_prev_away_score + 1
                goal_team_abbr = away_abbr
            else:
                mock_new_attrs["home_score"] = self.internal_prev_home_score + 1
                goal_team_abbr = home_abbr
        else:
            if my_abbr == home_abbr:
                mock_new_attrs["home_score"] = self.internal_prev_home_score + 1
            else:
                mock_new_attrs["away_score"] = self.internal_prev_away_score + 1
            goal_team_abbr = my_abbr

        mock_new_attrs["last_event"] = {
            "type": "goal",
            "team": goal_team_abbr,
            "timeInPeriod": "01:00",
            "periodOrd": "1st",
            "scorer": scorer_name,
            "assist1": assist1,
            "assist2": assist2,
            "strength": strength,
            "shotType": shot_type
        }
        mock_new_attrs["last_event_id"] = f"test_goal_{datetime.datetime.now().strftime('%H%M%S%f')}"
        mock_new_attrs["last_play"] = f"Goal: {scorer_name} ({goal_team_abbr}) at 01:00 of 1st (Assists: {assist1}, {assist2})"
        mock_new_state["attributes"] = mock_new_attrs

        scoring_feed = mock_new_attrs.get("scoring_detailed") or []
        goal_detail = self._match_goal_detail(scoring_feed, goal_team_abbr, f"{mock_new_attrs.get('home_score',0)}-{mock_new_attrs.get('away_score',0)}", scorer_name, "01:00")
        if not scoring_feed:
            scoring_feed = [{
                "team": goal_team_abbr,
                "scorer": scorer_name,
                "assists": [assist1, assist2],
                "shot_type": shot_type,
                "strength": strength,
                "time": "01:00",
                "period_ord": "1st",
                "score_str": f"{mock_new_attrs.get('home_score',0)}-{mock_new_attrs.get('away_score',0)}"
            }]
        mock_new_attrs["scoring_detailed"] = scoring_feed

        self.dashboard_sensor_change_callback(
            self.dashboard_sensor_entity_id,
            "attributes",
            mock_old_state,
            mock_new_state,
            {"is_test_trigger": True}
        )

        if self.entity_exists(entity):
            self.turn_off(entity)

    def test_win_callback(self, entity, attribute, old, new, kwargs):
        self.log_message("Team WIN test triggered.", level="INFO")
        try:
            preset = str(self.get_state(self.team_notification_preset_select) or "None")
            api_style = PRESET_TO_API_STYLE_NAME_MAP.get(preset, preset)
            my_abbr = NHL_TEAM_NAME_TO_ABBREV_MAP.get(api_style)
            my_name = NHL_TEAM_ABBREV_TO_FULL_NAME_MAP.get(my_abbr, "Home Team")
            dash = self.get_state(self.dashboard_sensor_entity_id, attribute="all") or {}
            attrs = dash.get("attributes", {}) if isinstance(dash, dict) else {}
            home_abbr = (attrs.get("home_abbr") or my_abbr or "HME")
            away_abbr = (attrs.get("away_abbr") or "AWY")
            is_home = (my_abbr == home_abbr)
            opp_abbr = away_abbr if is_home else home_abbr
            opp_name = NHL_TEAM_ABBREV_TO_FULL_NAME_MAP.get(opp_abbr, "Opponent")

            my_score = 4
            opp_score = 2
            if my_abbr == home_abbr:
                if isinstance(attrs.get("home_score"), int) and isinstance(attrs.get("away_score"), int):
                    my_score = max(attrs.get("home_score"), attrs.get("away_score") + 1)
                    opp_score = min(attrs.get("home_score"), attrs.get("away_score"))
            elif my_abbr == away_abbr:
                if isinstance(attrs.get("home_score"), int) and isinstance(attrs.get("away_score"), int):
                    my_score = max(attrs.get("away_score"), attrs.get("home_score") + 1)
                    opp_score = min(attrs.get("away_score"), attrs.get("home_score"))

            event = {
                "team_name": API_TO_STANDARD_TEAM_NAME_MAP.get(my_name, my_name),
                "my_team_name": my_name,
                "my_team_abbr": my_abbr or "HME",
                "opp_team_name": opp_name,
                "opp_team_abbr": opp_abbr or "AWY",
                "my_team_score": my_score,
                "opp_team_score": opp_score,
                "is_home": is_home,
                "source": f"nhl_notifications_app_v{self.APP_VERSION} (WIN_TEST)"
            }
            self.fire_event(self.team_win_event_to_fire, **event)
        finally:
            if self.entity_exists(entity):
                self.turn_off(entity)