"""
Extractor for on-ice players and penalty box data from NHL API.
"""

from typing import Dict, Any, List, Optional


class OnIceExtractor:
    """Extract on-ice players and penalty box information."""
    
    def extract_on_ice(
        self, 
        landing_data: Dict[str, Any],
        rosters: Dict[int, List[Any]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extract current on-ice players from landing summary.
        
        Args:
            landing_data: Landing endpoint data
            rosters: Rosters dict {team_id: [Player objects]}
            
        Returns:
            Dict with 'home' and 'away' lists of on-ice players
        """
        on_ice = {'home': [], 'away': []}
        
        try:
            ice_surface = landing_data.get('summary', {}).get('iceSurface', {})
            if not ice_surface:
                return on_ice
            
            # Get team IDs for roster lookup
            home_team_id = landing_data.get('homeTeam', {}).get('id')
            away_team_id = landing_data.get('awayTeam', {}).get('id')
            
            home_roster = rosters.get(home_team_id, [])
            away_roster = rosters.get(away_team_id, [])
            
            # Extract home team on-ice
            home_team_data = ice_surface.get('homeTeam', {})
            on_ice['home'] = self._extract_team_on_ice(home_team_data, home_roster)
            
            # Extract away team on-ice
            away_team_data = ice_surface.get('awayTeam', {})
            on_ice['away'] = self._extract_team_on_ice(away_team_data, away_roster)
            
        except Exception as e:
            print(f"Error extracting on-ice players: {e}")
        
        return on_ice
    
    def _extract_team_on_ice(
        self, 
        team_data: Dict[str, Any],
        roster: List[Any]
    ) -> List[Dict[str, Any]]:
        """Extract on-ice players for one team."""
        on_ice_players = []
        
        # Get forwards
        forwards = team_data.get('forwards', [])
        for player in forwards:
            player_info = self._get_on_ice_player_info(player, roster)
            if player_info:
                on_ice_players.append(player_info)
        
        # Get defensemen
        defensemen = team_data.get('defensemen', [])
        for player in defensemen:
            player_info = self._get_on_ice_player_info(player, roster)
            if player_info:
                on_ice_players.append(player_info)
        
        # Get goalie
        goalies = team_data.get('goalies', [])
        for player in goalies:
            player_info = self._get_on_ice_player_info(player, roster)
            if player_info:
                on_ice_players.append(player_info)
        
        return on_ice_players
    
    def _get_on_ice_player_info(
        self,
        ice_player: Dict[str, Any],
        roster: List[Any]
    ) -> Optional[Dict[str, Any]]:
        """Get full player info from roster by matching ID."""
        player_id = ice_player.get('playerId')
        if not player_id:
            return None
        
        # Find player in roster
        for player in roster:
            if player.player_id == player_id:
                return {
                    'id': player.player_id,
                    'name': player.full_name,
                    'sweater': player.sweater_number,
                    'position': player.position,
                    'position_code': ice_player.get('positionCode', player.position),
                    'headshot_url': player.headshot_url
                }
        
        # Player not found in roster - use basic info
        return {
            'id': player_id,
            'name': ice_player.get('name', {}).get('default', 'Unknown'),
            'sweater': ice_player.get('sweaterNumber', 0),
            'position': ice_player.get('positionCode', ''),
            'position_code': ice_player.get('positionCode', ''),
            'headshot_url': f"https://assets.nhle.com/mugs/nhl/20242025/{player_id}.png"
        }
    
    def extract_penalty_box(
        self,
        landing_data: Dict[str, Any],
        rosters: Dict[int, List[Any]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extract players currently in penalty box.
        
        Args:
            landing_data: Landing endpoint data
            rosters: Rosters dict {team_id: [Player objects]}
            
        Returns:
            Dict with 'home' and 'away' lists of penalized players
        """
        penalty_box = {'home': [], 'away': []}
        
        try:
            ice_surface = landing_data.get('summary', {}).get('iceSurface', {})
            if not ice_surface:
                return penalty_box
            
            # Get team IDs for roster lookup
            home_team_id = landing_data.get('homeTeam', {}).get('id')
            away_team_id = landing_data.get('awayTeam', {}).get('id')
            
            home_roster = rosters.get(home_team_id, [])
            away_roster = rosters.get(away_team_id, [])
            
            # Extract home team penalty box
            home_penalties = ice_surface.get('homeTeam', {}).get('penaltyBox', [])
            penalty_box['home'] = self._extract_team_penalty_box(home_penalties, home_roster)
            
            # Extract away team penalty box
            away_penalties = ice_surface.get('awayTeam', {}).get('penaltyBox', [])
            penalty_box['away'] = self._extract_team_penalty_box(away_penalties, away_roster)
            
        except Exception as e:
            print(f"Error extracting penalty box: {e}")
        
        return penalty_box
    
    def _extract_team_penalty_box(
        self,
        penalties: List[Dict[str, Any]],
        roster: List[Any]
    ) -> List[Dict[str, Any]]:
        """Extract penalty box players for one team."""
        penalty_players = []
        
        for penalty in penalties:
            player_id = penalty.get('playerId')
            if not player_id:
                continue
            
            # Find player in roster
            player_info = None
            for player in roster:
                if player.player_id == player_id:
                    player_info = {
                        'id': player.player_id,
                        'name': player.full_name,
                        'sweater': player.sweater_number,
                        'position': player.position,
                        'time_remaining': penalty.get('timeRemaining', ''),
                        'headshot_url': player.headshot_url
                    }
                    break
            
            # If not found in roster, use basic info
            if not player_info:
                player_info = {
                    'id': player_id,
                    'name': penalty.get('name', {}).get('default', 'Unknown'),
                    'sweater': penalty.get('sweaterNumber', 0),
                    'position': '',
                    'time_remaining': penalty.get('timeRemaining', ''),
                    'headshot_url': f"https://assets.nhle.com/mugs/nhl/20242025/{player_id}.png"
                }
            
            penalty_players.append(player_info)
        
        return penalty_players