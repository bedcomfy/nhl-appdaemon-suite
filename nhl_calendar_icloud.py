# /config/appdaemon/apps/nhl_calendar_icloud.py
import appdaemon.plugins.hass.hassapi as hass
import datetime
import json
import traceback
from typing import Any, Dict, List, Optional

# External libs (install via AppDaemon add-on python_packages)
import caldav
from caldav import DAVClient
from caldav.objects import Calendar, Event
import vobject  # iCalendar builder

# For robust UTC handling with vobject (avoids TZID guess errors)
try:
    from dateutil.tz import tzutc as _tzutc  # preferred UTC tzinfo for vobject
except Exception:
    _tzutc = None

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

# NHL API (per reference: https://api-web.nhle.com/)
import urllib.request
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError


TEAM_NAME_TO_ABBR = {
    "Anaheim Ducks": "ANA",
    "Arizona Coyotes": "ARI",
    "Boston Bruins": "BOS",
    "Buffalo Sabres": "BUF",
    "Calgary Flames": "CGY",
    "Carolina Hurricanes": "CAR",
    "Chicago Blackhawks": "CHI",
    "Colorado Avalanche": "COL",
    "Columbus Blue Jackets": "CBJ",
    "Dallas Stars": "DAL",
    "Detroit Red Wings": "DET",
    "Edmonton Oilers": "EDM",
    "Florida Panthers": "FLA",
    "Los Angeles Kings": "LAK",
    "Minnesota Wild": "MIN",
    "Montréal Canadiens": "MTL",
    "Montreal Canadiens": "MTL",
    "Nashville Predators": "NSH",
    "New Jersey Devils": "NJD",
    "New York Islanders": "NYI",
    "New York Rangers": "NYR",
    "Ottawa Senators": "OTT",
    "Philadelphia Flyers": "PHI",
    "Pittsburgh Penguins": "PIT",
    "San Jose Sharks": "SJS",
    "San José Sharks": "SJS",
    "Seattle Kraken": "SEA",
    "St. Louis Blues": "STL",
    "St Louis Blues": "STL",
    "Tampa Bay Lightning": "TBL",
    "Toronto Maple Leafs": "TOR",
    "Vancouver Canucks": "VAN",
    "Vegas Golden Knights": "VGK",
    "Washington Capitals": "WSH",
    "Winnipeg Jets": "WPG",
    "Utah Mammoth": "UTA",
    "Utah Hockey Club": "UTA",
}
ABBR_TO_FULL = {v: k for k, v in TEAM_NAME_TO_ABBR.items()}


class NhlCalendarIcloud(hass.Hass):
    APP_VERSION = "2.1.4-icloud"

    WEEK_URL = "https://api-web.nhle.com/v1/club-schedule/{abbr}/week/{key}"
    MONTH_URL = "https://api-web.nhle.com/v1/club-schedule/{abbr}/month/{key}"

    def initialize(self) -> None:
        try:
            self.log_level = (self.args.get("log_level") or "INFO").upper()

            # Required args
            self.team_preset = self.args.get("team_notification_preset_select")
            self.icloud_username = self.args.get("icloud_username")
            self.icloud_password = self.args.get("icloud_app_password")
            self.icloud_calendar_name = self.args.get("icloud_calendar_name")
            if not all(
                [
                    self.team_preset,
                    self.icloud_username,
                    self.icloud_password,
                    self.icloud_calendar_name,
                ]
            ):
                raise ValueError(
                    "Missing required args: team_notification_preset_select, icloud_username, icloud_app_password, icloud_calendar_name"
                )

            # Options
            self.days_ahead = int(self.args.get("days_ahead", 21))
            self.event_duration_minutes_default = int(
                self.args.get("event_duration_minutes_default", 150)
            )
            self.include_networks = bool(self.args.get("include_networks", True))
            self.include_venue = bool(self.args.get("include_venue", True))
            self.summary_prefix = str(self.args.get("summary_prefix", "")).strip()
            self.location_is_venue = bool(self.args.get("location_is_venue", True))
            self.clear_on_team_change = bool(
                self.args.get("clear_on_team_change", True)
            )
            self.sync_interval_minutes = int(
                self.args.get("sync_interval_minutes", 360)
            )
            self.test_sync_boolean = self.args.get(
                "test_sync_boolean", "input_boolean.nhl_test_calendar_sync"
            )
            # Track gid -> UID mapping + team in HA sensor
            self.state_sensor = self.args.get(
                "sync_state_sensor_entity_id", "sensor.nhl_calendar_sync_state"
            )

            # Timezone: prefer HA's configured IANA name; fall back safely
            tz_name = None
            try:
                tz_name = self.get_timezone()
            except Exception:
                tz_name = None

            if not tz_name:
                sys_tz = datetime.datetime.now().astimezone().tzinfo
                if hasattr(sys_tz, "key"):
                    tz_name = sys_tz.key
                else:
                    tz_name = "UTC"

            self.tz_name = tz_name or "UTC"
            if ZoneInfo:
                try:
                    self.tz = ZoneInfo(self.tz_name)
                except Exception:
                    self.tz = datetime.timezone.utc
                    self.tz_name = "UTC"
            else:
                self.tz = datetime.timezone.utc
                self.tz_name = "UTC"

            # Dedupe sensor
            self._ensure_state_sensor()

            # Connect to iCloud (calendar resolved lazily)
            self._client: Optional[DAVClient] = None
            self._calendar: Optional[Calendar] = None
            self._connect_icloud()

            # Listeners
            self.listen_state(self._team_changed_cb, self.team_preset)
            if self.test_sync_boolean and self.entity_exists(self.test_sync_boolean):
                self.listen_state(self._test_sync_cb, self.test_sync_boolean, new="on")

            # First sync
            self._timer = None
            self._schedule_next_sync(5)
            self.log_msg(
                f"iCloud NHL Publisher ready (v{self.APP_VERSION}) using TZ '{self.tz_name}'.",
                "INFO",
            )
        except Exception as e:
            self.log_msg(f"initialize() failed: {e}\n{traceback.format_exc()}", "ERROR")

    # ---------- logging ----------
    def log_msg(self, msg: str, level: str = "INFO"):
        self.log(f"[NHL_ICLOUD_V{self.APP_VERSION}] {msg}", level=level.upper())

    # ---------- state sensor ----------
    def _ensure_state_sensor(self):
        try:
            cur = self.get_state(self.state_sensor, attribute="all")
            if not cur or cur.get("state") in (None, "unknown", "unavailable"):
                self.set_state(
                    self.state_sensor, state="0", attributes={"map": {}, "team_abbr": None}
                )
        except Exception as e:
            self.log_msg(f"Could not create sensor {self.state_sensor}: {e}", "WARNING")

    def _load_map(self):
        all_state = self.get_state(self.state_sensor, attribute="all") or {}
        attrs = all_state.get("attributes") or {}
        return dict(attrs.get("map") or {}), attrs.get("team_abbr")

    def _save_map(self, m: Dict[str, str], team_abbr: Optional[str]):
        try:
            self.set_state(
                self.state_sensor, state=str(len(m)), attributes={"map": m, "team_abbr": team_abbr}
            )
        except Exception as e:
            self.log_msg(f"Failed to save map: {e}", "WARNING")

    # ---------- iCloud CalDAV ----------
    def _connect_icloud(self):
        try:
            self._client = caldav.DAVClient(
                url="https://caldav.icloud.com",
                username=self.icloud_username,
                password=self.icloud_password,
            )
            principal = self._client.principal()
            calendars = principal.calendars()
            for cal in calendars:
                try:
                    props = cal.get_properties([caldav.dav.DisplayName()])
                    name = str(props.get("{DAV:}displayname") or "")
                except Exception:
                    name = getattr(cal, "name", "") or ""
                if name == self.icloud_calendar_name:
                    self._calendar = cal
                    break
            if not self._calendar:
                raise RuntimeError(
                    f"Calendar named '{self.icloud_calendar_name}' not found in iCloud account."
                )
            self.log_msg(
                f"Connected to iCloud calendar '{self.icloud_calendar_name}'.", "INFO"
            )
        except Exception as e:
            self._client = None
            self._calendar = None
            self.log_msg(f"Failed to connect to iCloud CalDAV: {e}", "ERROR")

    def _ensure_calendar(self) -> Optional[Calendar]:
        if not self._client or not self._calendar:
            self._connect_icloud()
        return self._calendar

    def _find_event_by_uid(self, uid: str) -> Optional[Event]:
        cal = self._ensure_calendar()
        if not cal:
            return None
        try:
            events = cal.events()
            for ev in events:
                try:
                    raw = ev.data
                    if not raw:
                        continue
                    vcal = vobject.readOne(raw)
                    if hasattr(vcal, "vevent"):
                        ev_uid = str(getattr(vcal.vevent, "uid").value)
                        if ev_uid == uid:
                            return ev
                except Exception:
                    continue
        except Exception as e:
            self.log_msg(f"_find_event_by_uid error: {e}", "WARNING")
        return None

    # ---------- helpers ----------
    def _naive_in_tz(self, dt: datetime.datetime) -> datetime.datetime:
        """Return dt converted to self.tz, with tzinfo removed (local wall time)."""
        return dt.astimezone(self.tz).replace(tzinfo=None)

    # ---------- event create/update ----------
    def _create_or_update_event(
        self,
        uid: str,
        summary: str,
        description: str,
        location: Optional[str],
        start_local: datetime.datetime,
        end_local: datetime.datetime,
    ) -> bool:
        cal = self._ensure_calendar()
        if not cal:
            return False

        # Normalize input datetimes to tz-aware
        if start_local.tzinfo is None:
            start_local = start_local.replace(tzinfo=self.tz)
        if end_local.tzinfo is None:
            end_local = end_local.replace(tzinfo=self.tz)

        vcal = vobject.iCalendar()

        # VEVENT
        ve = vcal.add("vevent")
        ve.add("uid").value = uid
        ve.add("summary").value = summary
        if location:
            ve.add("location").value = location
        if description:
            ve.add("description").value = description

        # Strategy:
        # - Avoid emitting VTIMEZONE to dodge strict validator rules.
        # - If UTC: use a tzinfo that vobject recognizes (dateutil.tz.tzutc if available).
        # - If local zone: write floating local time and supply TZID param (no tzinfo on value).
        if self.tz_name.upper() == "UTC":
            utc_start = start_local.astimezone(datetime.timezone.utc)
            utc_end = end_local.astimezone(datetime.timezone.utc)

            if _tzutc:
                utc_start = utc_start.replace(tzinfo=_tzutc())
                utc_end = utc_end.replace(tzinfo=_tzutc())

                dtstart = ve.add("dtstart")
                dtstart.value = utc_start
                if "TZID" in dtstart.params:
                    del dtstart.params["TZID"]

                dtend = ve.add("dtend")
                dtend.value = utc_end
                if "TZID" in dtend.params:
                    del dtend.params["TZID"]
            else:
                # Fallback: write naive values and set TZID=UTC to prevent tz guessing
                dtstart = ve.add("dtstart")
                dtstart.value = utc_start.replace(tzinfo=None)
                dtstart.params["TZID"] = "UTC"

                dtend = ve.add("dtend")
                dtend.value = utc_end.replace(tzinfo=None)
                dtend.params["TZID"] = "UTC"
        else:
            # Local zone: floating local times with TZID param (no tzinfo)
            local_start = self._naive_in_tz(start_local)
            local_end = self._naive_in_tz(end_local)

            dtstart = ve.add("dtstart")
            dtstart.value = local_start
            dtstart.params["TZID"] = self.tz_name

            dtend = ve.add("dtend")
            dtend.value = local_end
            dtend.params["TZID"] = self.tz_name

        ics = vcal.serialize()

        existing = self._find_event_by_uid(uid)
        try:
            if existing:
                existing.data = ics
                existing.save()
            else:
                cal.add_event(ics)
            return True
        except Exception as e:
            self.log_msg(f"iCloud create/update failed for {uid}: {e}", "ERROR")
            return False

    def _delete_event_by_uid(self, uid: str) -> bool:
        ev = self._find_event_by_uid(uid)
        if not ev:
            return False
        try:
            ev.delete()
            return True
        except Exception as e:
            self.log_msg(f"Delete failed for {uid}: {e}", "WARNING")
            return False

    # ---------- NHL API ----------
    def _fetch_json(self, url: str) -> Optional[Dict[str, Any]]:
        try:
            req = Request(url, headers={"User-Agent": "AppDaemon-NHL-iCloud"})
            with urlopen(req, timeout=10) as resp:
                if getattr(resp, "status", 200) != 200:
                    self.log_msg(f"HTTP {getattr(resp, 'status', '??')} on {url}", "WARNING")
                    return None
                return json.loads(resp.read().decode("utf-8"))
        except (HTTPError, URLError) as e:
            self.log_msg(f"HTTP error on {url}: {e}", "WARNING")
        except Exception as e:
            self.log_msg(f"_fetch_json error on {url}: {e}", "ERROR")
        return None

    def _abbr_from_preset(self) -> Optional[str]:
        name = str(self.get_state(self.team_preset) or "").strip()
        return TEAM_NAME_TO_ABBR.get(name)

    def _gather_games(self, abbr: str) -> List[Dict[str, Any]]:
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        in7 = (now_utc + datetime.timedelta(days=7)).strftime("%Y-%m-%d")
        next_month_key = (now_utc.replace(day=1) + datetime.timedelta(days=35)).strftime("%Y-%m")
        urls = [
            self.WEEK_URL.format(abbr=abbr, key="now"),
            self.WEEK_URL.format(abbr=abbr, key=in7),
            self.MONTH_URL.format(abbr=abbr, key="now"),
            self.MONTH_URL.format(abbr=abbr, key=next_month_key),
        ]

        by_id: Dict[str, Dict[str, Any]] = {}
        for u in urls:
            data = self._fetch_json(u)
            if not data:
                continue
            for g in (data.get("games") or []):
                gid = str(g.get("id") or "")
                start = g.get("startTimeUTC")
                if not gid or not start:
                    continue
                if g.get("gameState") in ("PPD", "CNCL"):
                    continue
                if not self._within_window(start):
                    continue
                by_id.setdefault(gid, g)

        games = list(by_id.values())
        games.sort(key=lambda x: x.get("startTimeUTC") or "")
        return games

    def _within_window(self, start_utc: str) -> bool:
        try:
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            tgt = datetime.datetime.fromisoformat(str(start_utc).replace("Z", "+00:00"))
            delta = (tgt - now_utc).total_seconds()
            return (-6 * 3600) <= delta <= (self.days_ahead * 86400)
        except Exception:
            return False

    def _utc_to_local(self, utc_iso: str) -> Optional[datetime.datetime]:
        try:
            dt_utc = datetime.datetime.fromisoformat(str(utc_iso).replace("Z", "+00:00"))
            return dt_utc.astimezone(self.tz)
        except Exception:
            return None

    def _summary(self, my_abbr: str, home: str, away: str) -> str:
        pre = f"{self.summary_prefix} " if self.summary_prefix else ""
        if my_abbr == home:
            return f"{pre}{ABBR_TO_FULL.get(home, home)} vs {ABBR_TO_FULL.get(away, away)}"
        return f"{pre}{ABBR_TO_FULL.get(away, away)} at {ABBR_TO_FULL.get(home, home)}"

    def _location(self, game: Dict[str, Any]) -> Optional[str]:
        if not self.include_venue or not self.location_is_venue:
            return None
        v = game.get("venue")
        if isinstance(v, dict):
            return v.get("default") or v.get("name") or None
        return str(v) if v else None

    def _description(self, game: Dict[str, Any], gid: str) -> str:
        parts: List[str] = []
        if self.include_networks:
            try:
                nets = [b.get("network") for b in (game.get("tvBroadcasts") or []) if b.get("network")]
                if nets:
                    parts.append("TV: " + ", ".join(nets))
            except Exception:
                pass
        parts.append(f"NHLSYNC:{gid}")
        return " | ".join(parts)

    # ---------- scheduler ----------
    def _schedule_next_sync(self, in_seconds: int):
        if getattr(self, "_timer", None):
            try:
                if self.timer_running(self._timer):
                    self.cancel_timer(self._timer)
            except Exception:
                pass
        self._timer = self.run_in(lambda _: self._sync_now(), max(3, int(in_seconds)))

    # ---------- callbacks ----------
    def _team_changed_cb(self, entity, attribute, old, new, kwargs):
        if old == new or not new or new in ["unknown", "unavailable", "None", ""]:
            return
        self.log_msg(f"Team changed: {old} -> {new}", "INFO")
        if self.clear_on_team_change:
            m, prev = self._load_map()
            deleted = 0
            for gid, uid in list(m.items()):
                if self._delete_event_by_uid(uid):
                    deleted += 1
                m.pop(gid, None)
            self._save_map(m, None)
            self.log_msg(f"Deleted {deleted} events from iCloud for previous team.", "INFO")
        else:
            self._save_map({}, None)
        self._schedule_next_sync(2)

    def _test_sync_cb(self, entity, attribute, old, new, kwargs):
        self.log_msg("Manual test sync triggered.", "INFO")
        try:
            self._sync_now()
        finally:
            self.run_in(lambda _: self.turn_off(entity), 1)

    # ---------- main sync ----------
    def _sync_now(self):
        try:
            my_abbr = self._abbr_from_preset()
            if not my_abbr:
                self.log_msg("No valid team selected; skipping.", "WARNING")
                return

            if not self._ensure_calendar():
                self.log_msg("iCloud calendar not available; skipping.", "ERROR")
                return

            games = self._gather_games(my_abbr)
            if games is None:
                self.log_msg("Fetch returned no data; skipping sync.", "WARNING")
                return

            m, prev = self._load_map()

            # If previous team differs, clear remnants
            if prev and prev != my_abbr:
                for gid, uid in list(m.items()):
                    self._delete_event_by_uid(uid)
                    m.pop(gid, None)

            created = 0
            updated = 0
            seen = set()

            for g in games:
                gid = str(g.get("id"))
                seen.add(gid)

                home = (g.get("homeTeam") or {}).get("abbrev") or ""
                away = (g.get("awayTeam") or {}).get("abbrev") or ""
                start_local = self._utc_to_local(g.get("startTimeUTC"))
                if not start_local:
                    continue
                end_local = start_local + datetime.timedelta(minutes=self.event_duration_minutes_default)

                uid = m.get(gid) or f"nhl-{gid}@appdaemon"
                summary = self._summary(my_abbr, home, away)
                location = self._location(g)
                description = self._description(g, gid)

                ok = self._create_or_update_event(uid, summary, description, location, start_local, end_local)
                if ok:
                    if gid in m:
                        updated += 1
                    else:
                        created += 1
                        m[gid] = uid

            # Delete any tracked game that’s no longer in window
            removed = 0
            for gid in list(m.keys()):
                if gid not in seen:
                    if self._delete_event_by_uid(m[gid]):
                        removed += 1
                    m.pop(gid, None)

            self._save_map(m, my_abbr)
            self.log_msg(
                f"Sync complete. Created: {created}, Updated: {updated}, Removed: {removed}, Tracked: {len(m)}",
                "INFO",
            )
        except Exception as e:
            self.log_msg(f"_sync_now crashed: {e}\n{traceback.format_exc()}", "ERROR")
        finally:
            self._schedule_next_sync(self.sync_interval_minutes * 60)

    def terminate(self) -> None:
        if getattr(self, "_timer", None):
            try:
                if self.timer_running(self._timer):
                    self.cancel_timer(self._timer)
            except Exception:
                pass
        self.log_msg("Terminated.", "INFO")