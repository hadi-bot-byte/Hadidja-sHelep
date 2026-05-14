"""
Dispatch Service - Responder Matching Module
"""

import random
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

class MatchingStrategy:
    def find_responder(self, incident_id: str, location: Dict) -> Optional[str]:
        raise NotImplementedError

class SimpleMatcher(MatchingStrategy):
    def find_responder(self, incident_id: str, location: Dict) -> Optional[str]:
        logger.info(f"Matching responder for incident {incident_id}")
        return "r1"

class DistanceBasedMatcher(MatchingStrategy):
    def find_responder(self, incident_id: str, location: Dict) -> Optional[str]:
        responders_with_distance = [("r1", 1.0), ("r2", 2.0), ("r3", 3.0)]
        closest = min(responders_with_distance, key=lambda x: x[1])
        return closest[0]

class StrategyBasedMatcher:
    _strategies = {
        "simple": SimpleMatcher,
        "distance": DistanceBasedMatcher,
    }
    
    @classmethod
    def get_matcher(cls, strategy_name: str = "simple") -> MatchingStrategy:
        strategy_class = cls._strategies.get(strategy_name, SimpleMatcher)
        return strategy_class()

def matcher():
    return StrategyBasedMatcher.get_matcher("simple")

def haversine_m(lat1, lon1, lat2, lon2):
    # Simple distance calculation in meters
    return ((lat1 - lat2)**2 + (lon1 - lon2)**2)**0.5 * 111000
