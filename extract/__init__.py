"""
England Hockey Analytics - Extract Module
Web scraping with Selenium.
"""

from extract.config import (
    # Paths
    PROJECT_ROOT,
    DATA_DIR,
    SAMPLE_DIR,
    LOG_DIR,
    # URLs
    BASE_URL,
    COMPETITIONS_URL,
    STANDINGS_URL_PATTERN,
    FIXTURES_URL_PATTERN,
    MATCH_DETAILS_URL_PATTERN,
    # Config instances
    SELECTORS,
    RATE_LIMITS,
    SELENIUM_CONFIG,
    LOG_CONFIG,
    OUTPUT_FILES,
    # Row dataclasses
    StandingsRow,
    MatchRow,
    MatchEventRow,
)
from extract.competitions import (
    # Dataclass
    FilterConfig,
    # Constants
    SEASONS,
    CURRENT_SEASON,
    COMP_GROUPS,
    WOMENS_COMPETITIONS,
    MENS_COMPETITIONS,
    # Pre-built configs
    DEFAULT_FILTER,
    WOMENS_PREMIER_FILTER,
    MENS_PREMIER_FILTER,
    # Functions
    build_filter_config,
    get_all_competitions,
    list_competitions,
)
from extract.scrapers import BaseScraper, StandingsScraper
from extract.utils import setup_logger, get_logger, CSVWriter

__all__ = [
    # Paths
    "PROJECT_ROOT",
    "DATA_DIR",
    "SAMPLE_DIR",
    "LOG_DIR",
    # URLs
    "BASE_URL",
    "COMPETITIONS_URL",
    "STANDINGS_URL_PATTERN",
    "FIXTURES_URL_PATTERN",
    "MATCH_DETAILS_URL_PATTERN",
    # Competitions
    "FilterConfig",
    "SEASONS",
    "CURRENT_SEASON",
    "COMP_GROUPS",
    "WOMENS_COMPETITIONS",
    "MENS_COMPETITIONS",
    "DEFAULT_FILTER",
    "WOMENS_PREMIER_FILTER",
    "MENS_PREMIER_FILTER",
    "build_filter_config",
    "get_all_competitions",
    "list_competitions",
    # Config
    "SELECTORS",
    "RATE_LIMITS",
    "SELENIUM_CONFIG",
    "LOG_CONFIG",
    "OUTPUT_FILES",
    # Row types
    "StandingsRow",
    "MatchRow",
    "MatchEventRow",
    # Scrapers
    "BaseScraper",
    "StandingsScraper",
    # Utils
    "setup_logger",
    "get_logger",
    "CSVWriter",
]
