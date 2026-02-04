from __future__ import annotations
from typing import Dict
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from textblob import TextBlob

_analyzer = SentimentIntensityAnalyzer()

def score_text(text: str) -> Dict[str, float]:
    text = text or ""
    vader = _analyzer.polarity_scores(text)
    tb = TextBlob(text).sentiment.polarity
    return {
        "vader_compound": float(vader["compound"]),
        "textblob_polarity": float(tb),
        "sentiment_avg": float((vader["compound"] + tb) / 2.0),
    }
