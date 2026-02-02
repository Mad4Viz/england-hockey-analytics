"""
England Hockey Analytics - Scrapers Package
Individual scraper classes for different data types.
"""

from extract.scrapers.base import BaseScraper
from extract.scrapers.standings import StandingsScraper
from extract.scrapers.matches import MatchesScraper
from extract.scrapers.match_events import MatchEventsScraper

__all__ = [
    "BaseScraper",
    "StandingsScraper",
    "MatchesScraper",
    "MatchEventsScraper",
]
