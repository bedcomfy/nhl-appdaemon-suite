"""Extract media and broadcast information."""

from typing import Dict, List, Any
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from nhl_data_extraction.models.game_data import Broadcast

class MediaExtractor:
    """Extracts media and broadcast information."""
    
    @staticmethod
    def extract(landing_data: Dict[str, Any]) -> List[Broadcast]:
        """
        Extract TV broadcast information.
        
        Args:
            landing_data: Data from landing endpoint
            
        Returns:
            List of Broadcast objects
        """
        broadcasts = []
        
        tv_broadcasts = landing_data.get('tvBroadcasts', [])
        
        for broadcast in tv_broadcasts:
            broadcasts.append(Broadcast(
                network=broadcast.get('network', ''),
                market=broadcast.get('market', ''),
                country_code=broadcast.get('countryCode', '')
            ))
        
        return broadcasts