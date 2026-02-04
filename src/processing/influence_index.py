from __future__ import annotations
import math

def influence_weight(score: int) -> float:
    return float(math.log1p(max(int(score or 0), 0)))

def social_influence_index(sentiment: float, score: int) -> float:
    return float(sentiment * influence_weight(score))
