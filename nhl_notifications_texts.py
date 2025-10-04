import appdaemon.plugins.hass.hassapi as hass


class NhlNotificationTexts(hass.Hass):
    """
    Central template registry for NHL notifications.
    All strings can be overridden under `templates:` in apps.yaml.

    Supported keys:
      goal_title, goal_body, goal_tts
      opponent_goal_title, opponent_goal_body, opponent_goal_tts
      penalty_title, penalty_body, penalty_tts
      live_game_title, live_game_body
      win_title, win_body, win_tts
      controls_title, controls_body
      preset_change_title, preset_change_body
    """

    def initialize(self):
        self.log_level = (self.args.get("log_level") or "INFO").upper()
        self.templates = self.args.get("templates", {}) or {}
        keys = ", ".join(sorted(self.templates.keys())) if self.templates else "none"
        self.log_message(f"Loaded {len(self.templates)} template override(s). Keys: {keys}")

    def log_message(self, message: str, level: str = "INFO") -> None:
        self.log(f"[NHL_TEXT_TEMPLATES] {message}", level=level.upper())

    def render(self, key: str, default_template: str, **kwargs) -> str:
        template = self.templates.get(key, default_template)
        try:
            if isinstance(template, str):
                return template.format(**kwargs)
            return str(template)
        except Exception as exc:
            self.log_message(f"Template '{key}' format error: {exc}", level="WARNING")
            try:
                return (default_template or "").format(**kwargs)
            except Exception:
                return default_template or ""

    def get_template(self, key: str, default: str = "") -> str:
        return str(self.templates.get(key, default))