"""
England Hockey Analytics - Configuration
All URLs, selectors, rate limits, and constants for web scraping.
"""

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Final, Optional


# =============================================================================
# PROJECT PATHS
# =============================================================================

PROJECT_ROOT: Final[Path] = Path(__file__).parent.parent
DATA_DIR: Final[Path] = PROJECT_ROOT / "data"
# HOCKEY_OUTPUT_DIR env var allows Prefect flow to redirect output to pre_production/
SAMPLE_DIR: Final[Path] = Path(os.environ.get("HOCKEY_OUTPUT_DIR", str(DATA_DIR / "sample")))
TEST_DIR: Final[Path] = SAMPLE_DIR / "Test"
LOG_DIR: Final[Path] = PROJECT_ROOT / "logs"


# =============================================================================
# URLS
# =============================================================================

BASE_URL: Final[str] = "https://www.englandhockey.co.uk"
COMPETITIONS_URL: Final[str] = f"{BASE_URL}/competitions"

# URL patterns (use .format() with parameters)
STANDINGS_URL_PATTERN: Final[str] = (
    "{competitions_url}/{path}/table"
    "?season={season_uuid}"
    "&competition-group={comp_group_uuid}"
    "&competition={competition_uuid}"
)

FIXTURES_URL_PATTERN: Final[str] = (
    "{competitions_url}/{path}"
    "?match-day={match_day}"  # Format: YYYY-MM-DD
)

MATCH_DETAILS_URL_PATTERN: Final[str] = "{base_url}/fixtures/{match_uuid}"


# =============================================================================
# COMPETITION FILTERS
# =============================================================================
# NOTE: FilterConfig and all competition UUIDs are defined in competitions.py
# Import from there: from extract.competitions import WOMENS_PREMIER_FILTER, etc.


# =============================================================================
# CSS SELECTORS
# =============================================================================

@dataclass(frozen=True)
class Selectors:
    """CSS selectors for scraping website elements."""

    # Loader (wait for dismissal before interacting)
    loader: str = ".c-loader"

    # Filter dropdowns
    season_dropdown: str = "[data-filter='season']"
    competition_group_dropdown: str = "[data-filter='competition-group']"
    competition_dropdown: str = "[data-filter='competition']"
    dropdown_option: str = ".c-dropdown__option"

    # Standings table
    standings_table: str = ".c-table--standings"
    standings_row: str = ".c-table--standings tbody tr"
    standings_position: str = "td:nth-child(1)"
    standings_team: str = "td:nth-child(2)"
    standings_played: str = "td:nth-child(3)"
    standings_won: str = "td:nth-child(4)"
    standings_drawn: str = "td:nth-child(5)"
    standings_lost: str = "td:nth-child(6)"
    standings_goals_for: str = "td:nth-child(7)"
    standings_goals_against: str = "td:nth-child(8)"
    standings_goal_diff: str = "td:nth-child(9)"
    standings_points: str = "td:nth-child(10)"

    # Fixtures/Results container
    match_container: str = ".c-match-detail-card__container"
    match_card: str = ".c-fixture"

    # Team badges and names
    home_team_badge: str = ".c-fixture__badge-before"
    away_team_badge: str = ".c-fixture__badge-after"
    team_name: str = ".c-badge__label"

    # Scores (for completed matches)
    score_board: str = ".c-fixture__score-board"
    score_item: str = ".c-score__item"
    home_score: str = ".c-score__item:first-child"
    away_score: str = ".c-score__item:last-child"

    # Match time (for upcoming fixtures)
    match_time: str = ".c-fixture__time"

    # Date picker / timeline
    date_picker: str = ".c-date-picker-timeline"
    date_item: str = ".c-date-picker-timeline__item"
    date_selected: str = ".is-selected"

    # Venue (location with span containing venue name)
    venue: str = ".c-fixture__location"
    venue_text: str = ".c-fixture__location span"

    # Match details page - header with events
    match_header_team: str = ".c-match-header__results-team-name"
    match_header_logo: str = ".c-match-header__results-team-logo"
    match_header_score: str = ".c-match-header__results-score"

    # Match events (goals and cards) - in info board
    info_board_home: str = ".c-match-header__info-board-home"
    info_board_away: str = ".c-match-header__info-board-away"
    info_board_item: str = ".c-info-board-item__text"
    event_icon: str = "use"  # SVG use element with xlink:href
    event_time: str = ".u-font-bold"  # Contains minute like "20'"

    # Match link
    match_link: str = "a[href*='/fixtures/']"

    # Competition ribbon (shows competition name above matches in "All Competitions" view)
    ribbon: str = ".c-ribbon"
    ribbon_title: str = ".c-ribbon__title"
    ribbon_link: str = ".c-ribbon__title a"


SELECTORS = Selectors()


# =============================================================================
# RATE LIMITING
# =============================================================================

@dataclass(frozen=True)
class RateLimits:
    """Rate limiting configuration with random jitter."""

    # Base delays (seconds)
    page_load_min: float = 3.0
    page_load_max: float = 5.0
    filter_change_min: float = 2.0
    filter_change_max: float = 3.0
    match_detail_min: float = 2.0
    match_detail_max: float = 3.0
    error_backoff: float = 10.0

    # Jitter range (added to base delay)
    jitter_min: float = -1.0
    jitter_max: float = 1.0

    # Retry configuration
    max_retries: int = 3
    retry_backoff_multiplier: float = 2.0


RATE_LIMITS = RateLimits()


# =============================================================================
# SELENIUM CONFIGURATION
# =============================================================================

@dataclass(frozen=True)
class SeleniumConfig:
    """Selenium WebDriver configuration."""
    headless: bool = False  # Default OFF for testing
    window_width: int = 1920
    window_height: int = 1080
    page_load_timeout: int = 30
    implicit_wait: int = 10
    explicit_wait: int = 20


SELENIUM_CONFIG = SeleniumConfig()

# Chrome options to apply
CHROME_OPTIONS: Final[list[str]] = [
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    f"--window-size={SELENIUM_CONFIG.window_width},{SELENIUM_CONFIG.window_height}",
]

# User agent to spoof (modern Chrome on macOS)
USER_AGENT: Final[str] = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================

@dataclass(frozen=True)
class LogConfig:
    """Logging configuration."""
    log_file: str = "extract.log"
    log_level: str = "INFO"
    format: str = "%(asctime)s - %(levelname)s - %(message)s"
    date_format: str = "%Y-%m-%d %H:%M:%S"


LOG_CONFIG = LogConfig()


# =============================================================================
# OUTPUT FILES
# =============================================================================

@dataclass(frozen=True)
class OutputFiles:
    """Output CSV file names."""
    standings: str = "standings.csv"
    matches: str = "matches.csv"
    match_events: str = "match_events.csv"
    filter_options: str = "filter_options.json"


OUTPUT_FILES = OutputFiles()


# =============================================================================
# DATA ROW TYPES (for CSV output)
# =============================================================================

@dataclass
class StandingsRow:
    """Row structure for standings data."""
    season: str
    phase: str
    competition_group: str
    competition: str
    position: int
    team: str
    played: int
    won: int
    drawn: int
    lost: int
    goals_for: int
    goals_against: int
    goal_difference: int
    points: int
    standings_url: str  # URL to the standings page for verification
    scraped_at: str  # ISO format: YYYY-MM-DDTHH:MM:SS


@dataclass
class MatchRow:
    """Row structure for matches (fixtures and results combined)."""
    season: str
    phase: str
    competition_group: str
    competition: str
    match_date: str  # ISO format: YYYY-MM-DD
    match_time: str  # HH:MM
    home_team: str
    away_team: str
    home_score: Optional[int]  # None = fixture (not played yet)
    away_score: Optional[int]  # None = fixture (not played yet)
    venue: str
    match_url: str
    scraped_at: str  # ISO format: YYYY-MM-DDTHH:MM:SS


@dataclass
class MatchEventRow:
    """Row structure for match events (goals and cards)."""
    season: str  # e.g., "2024-2025"
    match_url: str
    match_date: str
    home_team: str
    away_team: str
    home_logo: str  # Team logo URL
    away_logo: str  # Team logo URL
    event_type: str  # 'goal' or 'card'
    event_subtype: str  # 'FG', 'PC', 'PS' for goals; 'Green', 'Yellow', 'Red' for cards
    team: str  # Team the event is attributed to
    player_name: str
    minute: int
    scraped_at: str  # ISO format: YYYY-MM-DDTHH:MM:SS
