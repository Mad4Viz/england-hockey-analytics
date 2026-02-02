"""
England Hockey Analytics - Match Events Scraper
Scrapes goal and card events from individual match detail pages.
"""

import re
from datetime import datetime
from typing import Callable, List, Optional, Tuple

from selenium.webdriver.common.by import By

from extract.scrapers.base import BaseScraper
from extract.config import SELECTORS, MatchEventRow


class MatchEventsScraper(BaseScraper):
    """
    Scrapes events (goals and cards) from match detail pages.

    Usage:
        # Get match URLs from matches.csv or MatchesScraper
        match_urls = ["https://www.englandhockey.co.uk/fixtures/abc123", ...]

        with MatchEventsScraper(headless=True) as scraper:
            events = scraper.scrape_matches(match_urls)
    """

    def __init__(self, headless: bool = None):
        """
        Initialize match events scraper.

        Args:
            headless: Run Chrome in headless mode (default: from config)
        """
        super().__init__(headless=headless, log_name="match_events")

    def scrape(self) -> List[MatchEventRow]:
        """Not used directly - use scrape_matches() instead."""
        raise NotImplementedError("Use scrape_matches(match_urls) instead")

    def scrape_matches(
        self,
        match_info: List[dict],
        on_match_complete: Optional[Callable[[List[MatchEventRow]], None]] = None,
    ) -> List[MatchEventRow]:
        """
        Scrape events from multiple match detail pages.

        Args:
            match_info: List of dicts with 'url' and 'season' keys
            on_match_complete: Optional callback called after each match with its events.
                               Signature: callback(events: List[MatchEventRow]) -> None
                               Use this for incremental saving.

        Returns:
            List of MatchEventRow objects
        """
        all_events = []

        for i, info in enumerate(match_info):
            url = info["url"]
            season = info.get("season", "")
            self.logger.info(f"Scraping match {i+1}/{len(match_info)}: {url}")

            try:
                events = self._scrape_single_match(url, season)
                all_events.extend(events)
                self.logger.info(f"Found {len(events)} events (total: {len(all_events)})")

                # Incremental save callback
                if events and on_match_complete:
                    on_match_complete(events)

            except Exception as e:
                self.logger.warning(f"Failed to scrape {url}: {e}")
                continue

        self.logger.info(f"Total events scraped: {len(all_events)}")
        return all_events

    def _navigate_to_match(self, match_url: str) -> None:
        """Navigate to match detail page (no loader on these pages)."""
        self.logger.info(f"Navigating to: {match_url}")
        self.driver.get(match_url)
        self._rate_limit_match_detail()

    def _scrape_single_match(self, match_url: str, season: str = "") -> List[MatchEventRow]:
        """Scrape events from a single match page."""
        self._navigate_to_match(match_url)

        # Temporarily disable implicit wait for faster extraction
        from extract.config import SELENIUM_CONFIG
        self.driver.implicitly_wait(0)

        try:
            # Get match metadata
            match_date = self._extract_match_date()
            home_team, away_team = self._extract_teams()
            home_logo, away_logo = self._extract_logos()

            if not home_team or not away_team:
                self.logger.warning(f"Could not extract teams from {match_url}")
                return []

            events = []

            # Extract home team events from info board
            home_events = self._extract_events_from_board(
                SELECTORS.info_board_home,
                home_team,
                match_url,
                match_date,
                home_team,
                away_team,
                home_logo,
                away_logo,
                is_home=True,
                season=season,
            )
            events.extend(home_events)

            # Extract away team events from info board
            away_events = self._extract_events_from_board(
                SELECTORS.info_board_away,
                away_team,
                match_url,
                match_date,
                home_team,
                away_team,
                home_logo,
                away_logo,
                is_home=False,
                season=season,
            )
            events.extend(away_events)

            return events
        finally:
            # Restore implicit wait
            self.driver.implicitly_wait(SELENIUM_CONFIG.implicit_wait)

    def _extract_teams(self) -> Tuple[Optional[str], Optional[str]]:
        """Extract home and away team names from match header."""
        try:
            team_elems = self.driver.find_elements(
                By.CSS_SELECTOR, SELECTORS.match_header_team
            )
            if len(team_elems) >= 2:
                home_team = team_elems[0].text.strip()
                away_team = team_elems[1].text.strip()
                return home_team, away_team
        except Exception as e:
            self.logger.warning(f"Failed to extract teams: {e}")

        return None, None

    def _extract_logos(self) -> Tuple[str, str]:
        """Extract team logo URLs from match header."""
        try:
            logos = self.driver.find_elements(
                By.CSS_SELECTOR, SELECTORS.match_header_logo
            )
            home_logo = logos[0].get_attribute("src") if len(logos) >= 1 else ""
            away_logo = logos[1].get_attribute("src") if len(logos) >= 2 else ""
            return home_logo, away_logo
        except Exception:
            return "", ""

    def _extract_match_date(self) -> str:
        """Extract match date from page."""
        try:
            # Look for date in .js-match-date element
            date_elem = self.driver.find_element(By.CSS_SELECTOR, ".js-match-date")
            date_text = date_elem.text.strip()
            # Format: "22 November 2025 | 14:00"
            # Extract date part and convert to ISO format
            match = re.match(r"(\d{1,2})\s+(\w+)\s+(\d{4})", date_text)
            if match:
                day, month_name, year = match.groups()
                months = {
                    "January": "01", "February": "02", "March": "03",
                    "April": "04", "May": "05", "June": "06",
                    "July": "07", "August": "08", "September": "09",
                    "October": "10", "November": "11", "December": "12"
                }
                month = months.get(month_name, "01")
                return f"{year}-{month}-{day.zfill(2)}"
        except Exception:
            pass

        return ""

    def _extract_events_from_board(
        self,
        board_selector: str,
        team: str,
        match_url: str,
        match_date: str,
        home_team: str,
        away_team: str,
        home_logo: str,
        away_logo: str,
        is_home: bool,
        season: str = "",
    ) -> List[MatchEventRow]:
        """Extract events from info board section (home or away)."""
        events = []

        try:
            board = self.driver.find_element(By.CSS_SELECTOR, board_selector)
            items = board.find_elements(By.CSS_SELECTOR, SELECTORS.info_board_item)
        except Exception:
            return []  # No events for this team

        for item in items:
            try:
                event = self._parse_info_board_item(
                    item, team, match_url, match_date,
                    home_team, away_team, home_logo, away_logo, is_home, season
                )
                if event:
                    events.append(event)
            except Exception as e:
                self.logger.debug(f"Failed to parse event item: {e}")
                continue

        return events

    def _parse_info_board_item(
        self,
        item,
        team: str,
        match_url: str,
        match_date: str,
        home_team: str,
        away_team: str,
        home_logo: str,
        away_logo: str,
        is_home: bool,
        season: str = "",
    ) -> Optional[MatchEventRow]:
        """Parse a single event from info board item."""
        # Get event type from SVG icon
        event_type, event_subtype = self._get_event_type(item)
        if not event_type:
            return None

        # Get minute from bold element
        minute = self._get_event_minute(item)

        # Get player name from text
        player_name = self._extract_player_name(item, is_home)

        return MatchEventRow(
            season=season,
            match_url=match_url,
            match_date=match_date,
            home_team=home_team,
            away_team=away_team,
            home_logo=home_logo,
            away_logo=away_logo,
            event_type=event_type,
            event_subtype=event_subtype,
            team=team,
            player_name=player_name,
            minute=minute,
            scraped_at=datetime.now().isoformat(timespec="seconds"),
        )

    def _get_event_type(self, item) -> Tuple[Optional[str], str]:
        """
        Extract event type from SVG icon.

        Returns:
            Tuple of (event_type, event_subtype) or (None, "") if unknown
        """
        try:
            use_elem = item.find_element(By.CSS_SELECTOR, SELECTORS.event_icon)
            href = use_elem.get_attribute("xlink:href") or use_elem.get_attribute("href")

            if not href:
                return None, ""

            # Parse icon type from href like "/assets/images/icons.svg#field-goal"
            icon_type = href.split("#")[-1] if "#" in href else href

            # Map to event types
            if icon_type == "field-goal":
                return "goal", "FG"
            elif icon_type == "penalty-corner":
                return "goal", "PC"
            elif icon_type == "penalty-stroke":
                return "goal", "PS"
            elif icon_type == "green-card":
                return "card", "Green"
            elif icon_type == "yellow-card":
                return "card", "Yellow"
            elif icon_type == "red-card":
                return "card", "Red"
            else:
                self.logger.debug(f"Unknown icon type: {icon_type}")
                return None, ""

        except Exception:
            return None, ""

    def _get_event_minute(self, item) -> int:
        """Extract minute from event item."""
        try:
            time_elem = item.find_element(By.CSS_SELECTOR, SELECTORS.event_time)
            time_text = time_elem.text.strip()
            # Parse "15'" -> 15
            minute_match = re.match(r"(\d+)", time_text)
            if minute_match:
                return int(minute_match.group(1))
        except Exception:
            pass

        return 0

    def _extract_player_name(self, item, is_home: bool) -> str:
        """
        Extract player name from event item.

        Home format: "Player Name 20'"
        Away format: "20' Player Name"
        """
        try:
            full_text = item.text.strip()
            # Remove minute (digits followed by various quote chars: ' ′ ')
            name = re.sub(r"\d+['\u2032\u2019]", "", full_text)
            # Clean up whitespace and newlines
            name = " ".join(name.split())
            return name.strip()
        except Exception:
            return ""
