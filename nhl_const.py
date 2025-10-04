# /config/appdaemon/apps/nhl_const.py
"""
This file contains shared constants and mappings for the NHL AppDaemon suite.
By centralizing these, we create a single source of truth and reduce code duplication.

Version 4.0.5: Added shared period formatter
"""

# --- Shared Period Formatter ---
def format_period_ordinal(period_num: int, period_type: str = "REG") -> str:
    """
    Shared period formatter used across all apps.
    
    Args:
        period_num: Period number (1, 2, 3, 4, 5...)
        period_type: "REG", "OT", or "SO"
    
    Returns:
        Formatted string: "1st", "2nd", "3rd", "OT", "2OT", "SO"
    """
    if not isinstance(period_num, int):
        try:
            period_num = int(period_num)
        except:
            return str(period_num)
    
    period_type = (period_type or "REG").upper()
    
    # Shootout
    if period_type == "SO":
        return "SO"
    
    # Overtime
    if period_type == "OT":
        if period_num == 4:
            return "OT"
        elif period_num > 4:
            return f"{period_num - 3}OT"
    
    # Regular periods
    if period_num == 1:
        return "1st"
    elif period_num == 2:
        return "2nd"
    elif period_num == 3:
        return "3rd"
    else:
        return f"{period_num}th"


# --- For nhl_goal_app.py (Lightshow Colors) ---
TEAM_COLORS = {
    "Anaheim Ducks":         [{"name":"Orange","r":255,"g":102,"b":0},{"name":"Gold","r":255,"g":215,"b":0},{"name":"Black","r":0,"g":0,"b":0}],
    "Arizona Coyotes":       [{"name":"Burgundy","r":255,"g":0,"b":51},{"name":"Sand","r":255,"g":204,"b":153},{"name":"Black","r":0,"g":0,"b":0}],
    "Boston Bruins":         [{"name":"Gold","r":255,"g":204,"b":0},{"name":"White","r":255,"g":255,"b":255},{"name":"Black","r":0,"g":0,"b":0}],
    "Buffalo Sabres":        [{"name":"Royal Blue","r":0,"g":0,"b":255},{"name":"Gold","r":255,"g":204,"b":0},{"name":"White","r":255,"g":255,"b":255}],
    "Calgary Flames":        [{"name":"Red","r":255,"g":0,"b":0},{"name":"Gold","r":255,"g":204,"b":0},{"name":"Black","r":0,"g":0,"b":0}],
    "Carolina Hurricanes":   [{"name":"Red","r":255,"g":0,"b":0},{"name":"White","r":255,"g":255,"b":255},{"name":"Black","r":0,"g":0,"b":0}],
    "Chicago Blackhawks":    [{"name":"Red","r":255,"g":0,"b":0},{"name":"Gold","r":255,"g":215,"b":0},{"name":"White","r":255,"g":255,"b":255}],
    "Colorado Avalanche":    [{"name":"Burgundy","r":255,"g":0,"b":51},{"name":"Blue","r":0,"g":0,"b":255},{"name":"White","r":255,"g":255,"b":255}],
    "Columbus Blue Jackets": [{"name":"Navy Blue","r":0,"g":0,"b":255},{"name":"Red","r":255,"g":0,"b":0},{"name":"White","r":255,"g":255,"b":255}],
    "Dallas Stars":          [{"name":"Victory Green","r":0,"g":255,"b":0},{"name":"White","r":255,"g":255,"b":255},{"name":"Black","r":0,"g":0,"b":0}],
    "Detroit Red Wings":     [{"name":"Red","r":255,"g":0,"b":0},{"name":"White","r":255,"g":255,"b":255},{"name":"Black","r":0,"g":0,"b":0}],
    "Edmonton Oilers":       [{"name":"Orange","r":255,"g":102,"b":0},{"name":"Blue","r":0,"g":0,"b":255},{"name":"White","r":255,"g":255,"b":255}],
    "Florida Panthers":      [{"name":"Red","r":255,"g":0,"b":0},{"name":"Blue","r":0,"g":0,"b":255},{"name":"Gold","r":255,"g":204,"b":0}],
    "Los Angeles Kings":     [{"name":"Silver","r":192,"g":192,"b":192},{"name":"White","r":255,"g":255,"b":255},{"name":"Black","r":0,"g":0,"b":0}],
    "Minnesota Wild":        [{"name":"Forest Green","r":0,"g":128,"b":0},{"name":"Red","r":255,"g":0,"b":0},{"name":"Wheat","r":245,"g":222,"b":179}],
    "Montreal Canadiens":    [{"name":"Red","r":255,"g":0,"b":0},{"name":"Blue","r":0,"g":0,"b":255},{"name":"White","r":255,"g":255,"b":255}],
    "Nashville Predators":   [{"name":"Gold","r":255,"g":204,"b":0},{"name":"Navy","r":0,"g":0,"b":128},{"name":"White","r":255,"g":255,"b":255}],
    "New Jersey Devils":     [{"name":"Red","r":255,"g":0,"b":0},{"name":"White","r":255,"g":255,"b":255},{"name":"Black","r":0,"g":0,"b":0}],
    "New York Islanders":    [{"name":"Royal Blue","r":0,"g":0,"b":255},{"name":"Orange","r":255,"g":102,"b":0},{"name":"White","r":255,"g":255,"b":255}],
    "New York Rangers":      [{"name":"Blue","r":0,"g":0,"b":255},{"name":"Red","r":255,"g":0,"b":0},{"name":"White","r":255,"g":255,"b":255}],
    "Ottawa Senators":       [{"name":"Red","r":255,"g":0,"b":0},{"name":"Gold","r":255,"g":204,"b":0},{"name":"Black","r":0,"g":0,"b":0}],
    "Philadelphia Flyers":   [{"name":"Orange","r":255,"g":102,"b":0},{"name":"White","r":255,"g":255,"b":255},{"name":"Black","r":0,"g":0,"b":0}],
    "Pittsburgh Penguins":   [{"name":"Gold","r":255,"g":204,"b":0},{"name":"White","r":255,"g":255,"b":255},{"name":"Black","r":0,"g":0,"b":0}],
    "San Jose Sharks":       [{"name":"Teal","r":0,"g":128,"b":128},{"name":"Orange","r":255,"g":102,"b":0},{"name":"Black","r":0,"g":0,"b":0}],
    "Seattle Kraken":        [{"name":"Deep Sea Blue","r":0,"g":0,"b":139},{"name":"Ice Blue","r":173,"g":216,"b":230},{"name":"Red","r":255,"g":0,"b":0}],
    "St Louis Blues":        [{"name":"Blue","r":0,"g":0,"b":255},{"name":"Yellow","r":255,"g":255,"b":0},{"name":"White","r":255,"g":255,"b":255}],
    "Tampa Bay Lightning":   [{"name":"Blue","r":0,"g":0,"b":255},{"name":"White","r":255,"g":255,"b":255},{"name":"Black","r":0,"g":0,"b":0}],
    "Toronto Maple Leafs":   [{"name":"Blue","r":0,"g":0,"b":255},{"name":"White","r":255,"g":255,"b":255},{"name":"Black","r":0,"g":0,"b":0}],
    "Utah Mammoth":          [{"name":"Mountain Blue","r":80,"g":142,"b":208},{"name":"Salt White","r":245,"g":245,"b":245},{"name":"Rock Black","r":10,"g":10,"b":10}],
    "Vancouver Canucks":     [{"name":"Blue","r":0,"g":0,"b":255},{"name":"Green","r":0,"g":255,"b":0},{"name":"White","r":255,"g":255,"b":255}],
    "Vegas Golden Knights":  [{"name":"Gold","r":255,"g":215,"b":0},{"name":"Red","r":255,"g":0,"b":0},{"name":"Black","r":0,"g":0,"b":0}],
    "Washington Capitals":   [{"name":"Red","r":255,"g":0,"b":0},{"name":"Blue","r":0,"g":0,"b":255},{"name":"White","r":255,"g":255,"b":255}],
    "Winnipeg Jets":         [{"name":"Navy Blue","r":0,"g":0,"b":128},{"name":"Light Blue","r":173,"g":216,"b":230},{"name":"White","r":255,"g":255,"b":255}]
}

DEFAULT_COLORS_LIST = [
    {"name":"Default Red","r":255,"g":0,"b":0},
    {"name":"Default White","r":255,"g":255,"b":255},
    {"name":"Default Black","r":0,"g":0,"b":0}
]

# --- For nhl_goal_app.py (Event Name Normalization) ---
EVENT_NAME_TO_STANDARD_KEY_MAP = {
    # Full names (existing)
    "Anaheim Ducks": "Anaheim Ducks",
    "Arizona Coyotes": "Arizona Coyotes",
    "Boston Bruins": "Boston Bruins",
    "Buffalo Sabres": "Buffalo Sabres",
    "Calgary Flames": "Calgary Flames",
    "Carolina Hurricanes": "Carolina Hurricanes",
    "Chicago Blackhawks": "Chicago Blackhawks",
    "Colorado Avalanche": "Colorado Avalanche",
    "Columbus Blue Jackets": "Columbus Blue Jackets",
    "Dallas Stars": "Dallas Stars",
    "Detroit Red Wings": "Detroit Red Wings",
    "Edmonton Oilers": "Edmonton Oilers",
    "Florida Panthers": "Florida Panthers",
    "Los Angeles Kings": "Los Angeles Kings",
    "Minnesota Wild": "Minnesota Wild",
    "Montreal Canadiens": "Montreal Canadiens",
    "Montréal Canadiens": "Montreal Canadiens",
    "Montral Canadiens": "Montreal Canadiens",
    "Nashville Predators": "Nashville Predators",
    "New Jersey Devils": "New Jersey Devils",
    "New York Islanders": "New York Islanders",
    "New York Rangers": "New York Rangers",
    "Ottawa Senators": "Ottawa Senators",
    "Philadelphia Flyers": "Philadelphia Flyers",
    "Pittsburgh Penguins": "Pittsburgh Penguins",
    "San Jose Sharks": "San Jose Sharks",
    "Seattle Kraken": "Seattle Kraken",
    "St Louis Blues": "St Louis Blues",
    "St. Louis Blues": "St Louis Blues",
    "Tampa Bay Lightning": "Tampa Bay Lightning",
    "Toronto Maple Leafs": "Toronto Maple Leafs",
    "Utah Mammoth": "Utah Mammoth",
    "Utah Hockey Club": "Utah Mammoth",
    "Vancouver Canucks": "Vancouver Canucks",
    "Vegas Golden Knights": "Vegas Golden Knights",
    "Washington Capitals": "Washington Capitals",
    "Winnipeg Jets": "Winnipeg Jets",
    
    # Short names
    "Ducks": "Anaheim Ducks",
    "Coyotes": "Arizona Coyotes",
    "Bruins": "Boston Bruins",
    "Sabres": "Buffalo Sabres",
    "Flames": "Calgary Flames",
    "Hurricanes": "Carolina Hurricanes",
    "Blackhawks": "Chicago Blackhawks",
    "Avalanche": "Colorado Avalanche",
    "Blue Jackets": "Columbus Blue Jackets",
    "Jackets": "Columbus Blue Jackets",
    "Stars": "Dallas Stars",
    "Red Wings": "Detroit Red Wings",
    "Wings": "Detroit Red Wings",
    "Oilers": "Edmonton Oilers",
    "Panthers": "Florida Panthers",
    "Kings": "Los Angeles Kings",
    "Wild": "Minnesota Wild",
    "Canadiens": "Montreal Canadiens",
    "Habs": "Montreal Canadiens",
    "Predators": "Nashville Predators",
    "Preds": "Nashville Predators",
    "Devils": "New Jersey Devils",
    "Islanders": "New York Islanders",
    "Isles": "New York Islanders",
    "Rangers": "New York Rangers",
    "Senators": "Ottawa Senators",
    "Sens": "Ottawa Senators",
    "Flyers": "Philadelphia Flyers",
    "Penguins": "Pittsburgh Penguins",
    "Pens": "Pittsburgh Penguins",
    "Sharks": "San Jose Sharks",
    "Kraken": "Seattle Kraken",
    "Blues": "St Louis Blues",
    "Lightning": "Tampa Bay Lightning",
    "Bolts": "Tampa Bay Lightning",
    "Maple Leafs": "Toronto Maple Leafs",
    "Leafs": "Toronto Maple Leafs",
    "Hockey Club": "Utah Mammoth",
    "Mammoth": "Utah Mammoth",
    "Canucks": "Vancouver Canucks",
    "Nucks": "Vancouver Canucks",
    "Golden Knights": "Vegas Golden Knights",
    "Knights": "Vegas Golden Knights",
    "Capitals": "Washington Capitals",
    "Caps": "Washington Capitals",
    "Jets": "Winnipeg Jets",
}

# --- Team mappings ---
NHL_TEAM_ABBREV_TO_FULL_NAME_MAP = {
    "ANA": "Anaheim Ducks", "ARI": "Arizona Coyotes", "BOS": "Boston Bruins",
    "BUF": "Buffalo Sabres", "CGY": "Calgary Flames", "CAR": "Carolina Hurricanes",
    "CHI": "Chicago Blackhawks", "COL": "Colorado Avalanche", "CBJ": "Columbus Blue Jackets",
    "DAL": "Dallas Stars", "DET": "Detroit Red Wings", "EDM": "Edmonton Oilers",
    "FLA": "Florida Panthers", "LAK": "Los Angeles Kings", "MIN": "Minnesota Wild",
    "MTL": "Montréal Canadiens",
    "NSH": "Nashville Predators", "NJD": "New Jersey Devils",
    "NYI": "New York Islanders", "NYR": "New York Rangers", "OTT": "Ottawa Senators",
    "PHI": "Philadelphia Flyers", "PIT": "Pittsburgh Penguins", "SJS": "San Jose Sharks",
    "SEA": "Seattle Kraken", "STL": "St. Louis Blues", "TBL": "Tampa Bay Lightning",
    "TOR": "Toronto Maple Leafs",
    "UTA": "Utah Hockey Club",
    "VAN": "Vancouver Canucks", "VGK": "Vegas Golden Knights", "WSH": "Washington Capitals",
    "WPG": "Winnipeg Jets",
    "NONE": "None"
}
NHL_TEAM_NAME_TO_ABBREV_MAP = {v: k for k, v in NHL_TEAM_ABBREV_TO_FULL_NAME_MAP.items()}

PRESET_TO_API_STYLE_NAME_MAP = {
    "Anaheim Ducks": "Anaheim Ducks", "Arizona Coyotes": "Arizona Coyotes", "Boston Bruins": "Boston Bruins",
    "Buffalo Sabres": "Buffalo Sabres", "Calgary Flames": "Calgary Flames", "Carolina Hurricanes": "Carolina Hurricanes",
    "Chicago Blackhawks": "Chicago Blackhawks", "Colorado Avalanche": "Colorado Avalanche", "Columbus Blue Jackets": "Columbus Blue Jackets",
    "Dallas Stars": "Dallas Stars", "Detroit Red Wings": "Detroit Red Wings", "Edmonton Oilers": "Edmonton Oilers",
    "Florida Panthers": "Florida Panthers", "Los Angeles Kings": "Los Angeles Kings", "Minnesota Wild": "Minnesota Wild",
    "Montréal Canadiens": "Montréal Canadiens", "Nashville Predators": "Nashville Predators",
    "New Jersey Devils": "New Jersey Devils", "New York Islanders": "New York Islanders", "New York Rangers": "New York Rangers",
    "Ottawa Senators": "Ottawa Senators", "Philadelphia Flyers": "Philadelphia Flyers", "Pittsburgh Penguins": "Pittsburgh Penguins",
    "San Jose Sharks": "San Jose Sharks", "Seattle Kraken": "Seattle Kraken", "St. Louis Blues": "St. Louis Blues",
    "Tampa Bay Lightning": "Tampa Bay Lightning", "Toronto Maple Leafs": "Toronto Maple Leafs",
    "Utah Mammoth": "Utah Hockey Club",
    "Vancouver Canucks": "Vancouver Canucks", "Vegas Golden Knights": "Vegas Golden Knights",
    "Washington Capitals": "Washington Capitals", "Winnipeg Jets": "Winnipeg Jets", "None": "None"
}

API_TO_STANDARD_TEAM_NAME_MAP = {
    "Montréal Canadiens": "Montreal Canadiens",
    "Utah Hockey Club": "Utah Mammoth",
    "St. Louis Blues": "St Louis Blues"
}

STANDARD_NAME_TO_PUSHOVER_SOUND_MAP = {
    "Anaheim Ducks": "Ducks", "Arizona Coyotes": "Coyotes", "Boston Bruins": "Bruins",
    "Buffalo Sabres": "Sabres", "Calgary Flames": "Flames", "Carolina Hurricanes": "Hurricanes",
    "Chicago Blackhawks": "Blackhawks", "Colorado Avalanche": "Avalanche", "Columbus Blue Jackets": "Jackets",
    "Dallas Stars": "Stars", "Detroit Red Wings": "Wings", "Edmonton Oilers": "Oilers",
    "Florida Panthers": "Panthers", "Los Angeles Kings": "Kings", "Minnesota Wild": "Wild",
    "Montreal Canadiens": "Canadiens", "Nashville Predators": "Predators", "New Jersey Devils": "Devils",
    "New York Islanders": "Islanders", "New York Rangers": "Rangers", "Ottawa Senators": "Senators",
    "Philadelphia Flyers": "Flyers", "Pittsburgh Penguins": "Penguins", "San Jose Sharks": "Sharks",
    "Seattle Kraken": "Kraken", "St Louis Blues": "Blues", "Tampa Bay Lightning": "Lightning",
    "Toronto Maple Leafs": "Leafs", "Utah Mammoth": "Mammoth", "Vancouver Canucks": "Canucks",
    "Vegas Golden Knights": "Knights", "Washington Capitals": "Capitals", "Winnipeg Jets": "Jets"
}

# --- Dashboard team details ---
NHL_TEAM_DETAILS_MAP = {
    "ANA": {"full_name": "Anaheim Ducks", "logo_id": "ANA", "colors": ["#F47A38", "#B9975B", "#000000"]},
    "ARI": {"full_name": "Arizona Coyotes", "logo_id": "ARI", "colors": ["#8C2633", "#E2D6B5", "#000000"]},
    "BOS": {"full_name": "Boston Bruins", "logo_id": "BOS", "colors": ["#FFB81C", "#000000", "#FFFFFF"]},
    "BUF": {"full_name": "Buffalo Sabres", "logo_id": "BUF", "colors": ["#002654", "#FCB514", "#ADAFAA"]},
    "CGY": {"full_name": "Calgary Flames", "logo_id": "CGY", "colors": ["#C8102E", "#F1BE48", "#000000"]},
    "CAR": {"full_name": "Carolina Hurricanes", "logo_id": "CAR", "colors": ["#CC0000", "#000000", "#A2AAAD"]},
    "CHI": {"full_name": "Chicago Blackhawks", "logo_id": "CHI", "colors": ["#CF0A2C", "#000000", "#FFD700"]},
    "COL": {"full_name": "Colorado Avalanche", "logo_id": "COL", "colors": ["#6F263D", "#236192", "#A2AAAD"]},
    "CBJ": {"full_name": "Columbus Blue Jackets", "logo_id": "CBJ", "colors": ["#002654", "#CE1126", "#A4A9AD"]},
    "DAL": {"full_name": "Dallas Stars", "logo_id": "DAL", "colors": ["#006847", "#8F8F8C", "#000000"]},
    "DET": {"full_name": "Detroit Red Wings", "logo_id": "DET", "colors": ["#CE1126", "#FFFFFF", "#F5F5F5"]},
    "EDM": {"full_name": "Edmonton Oilers", "logo_id": "EDM", "colors": ["#FF4C00", "#041E42", "#FFFFFF"]},
    "FLA": {"full_name": "Florida Panthers", "logo_id": "FLA", "colors": ["#041E42", "#C8102E", "#B9975B"]},
    "LAK": {"full_name": "Los Angeles Kings", "logo_id": "LAK", "colors": ["#111111", "#A2AAAD", "#FFFFFF"]},
    "MIN": {"full_name": "Minnesota Wild", "logo_id": "MIN", "colors": ["#154734", "#A6192E", "#EAAA00"]},
    "MTL": {"full_name": "Montréal Canadiens", "logo_id": "MTL", "colors": ["#AF1E2D", "#192168", "#FFFFFF"]},
    "NSH": {"full_name": "Nashville Predators", "logo_id": "NSH", "colors": ["#FFB81C", "#041E42", "#FFFFFF"]},
    "NJD": {"full_name": "New Jersey Devils", "logo_id": "NJD", "colors": ["#CE1126", "#000000", "#FFFFFF"]},
    "NYI": {"full_name": "New York Islanders", "logo_id": "NYI", "colors": ["#00539B", "#F47D30", "#FFFFFF"]},
    "NYR": {"full_name": "New York Rangers", "logo_id": "NYR", "colors": ["#0038A8", "#CE1126", "#FFFFFF"]},
    "OTT": {"full_name": "Ottawa Senators", "logo_id": "OTT", "colors": ["#C52032", "#000000", "#C2912C"]},
    "PHI": {"full_name": "Philadelphia Flyers", "logo_id": "PHI", "colors": ["#F74902", "#000000", "#FFFFFF"]},
    "PIT": {"full_name": "Pittsburgh Penguins", "logo_id": "PIT", "colors": ["#FCB514", "#000000", "#FFFFFF"]},
    "SJS": {"full_name": "San Jose Sharks", "logo_id": "SJS", "colors": ["#006D75", "#EA7200", "#000000"]},
    "SEA": {"full_name": "Seattle Kraken", "logo_id": "SEA", "colors": ["#001F5B", "#99D9D9", "#E9072B"]},
    "STL": {"full_name": "St. Louis Blues", "logo_id": "STL", "colors": ["#002F87", "#FCB514", "#041E42"]},
    "TBL": {"full_name": "Tampa Bay Lightning", "logo_id": "TBL", "colors": ["#002868", "#FFFFFF", "#C0C0C0"]},
    "TOR": {"full_name": "Toronto Maple Leafs", "logo_id": "TOR", "colors": ["#00205B", "#FFFFFF", "#8D9093"]},
    "UTA": {"full_name": "Utah Mammoth", "logo_id": "UTA", "colors": ["#002F57", "#E0E0E0", "#000000"]},
    "VAN": {"full_name": "Vancouver Canucks", "logo_id": "VAN", "colors": ["#00205B", "#00843D", "#97999B"]},
    "VGK": {"full_name": "Vegas Golden Knights", "logo_id": "VGK", "colors": ["#B4975A", "#333F42", "#000000"]},
    "WSH": {"full_name": "Washington Capitals", "logo_id": "WSH", "colors": ["#C8102E", "#041E42", "#FFFFFF"]},
    "WPG": {"full_name": "Winnipeg Jets", "logo_id": "WPG", "colors": ["#041E42", "#004C97", "#AC162C"]},
    "NHL": {"full_name": "NHL", "logo_id": "NHL", "colors": ["#7C8082", "#000000", "#D0D2D3"]}
}