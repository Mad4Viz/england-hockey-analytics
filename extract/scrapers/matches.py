"""
England Hockey Analytics - Matches Scraper
Scrapes fixtures and results from the England Hockey website.

Supports two modes:
1. AllCompetitionsConfig: Scrapes ALL competitions for a gender/season in one page
2. FilterConfig: Scrapes a specific competition (legacy mode)
"""

import re
from datetime import date, datetime
from typing import Callable, Dict, List, Optional, Tuple, Union, DefaultDict
from collections import defaultdict

from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement

from extract.scrapers.base import BaseScraper
from extract.config import (
    COMPETITIONS_URL,
    SELECTORS,
    MatchRow,
)
from extract.competitions import FilterConfig, AllCompetitionsConfig


class MatchesScraper(BaseScraper):
    """
    Scrapes fixtures and results from the matches page.

    Usage (All Competitions - recommended):
        from extract.competitions import build_all_competitions_config

        config = build_all_competitions_config("womens", "2024-2025")
        with MatchesScraper(config) as scraper:
            matches = scraper.scrape()

    Usage (Specific Competition - legacy):
        from extract.competitions import WOMENS_PREMIER_FILTER

        with MatchesScraper(WOMENS_PREMIER_FILTER) as scraper:
            matches = scraper.scrape()
    """

    def __init__(
        self,
        config: Union[AllCompetitionsConfig, FilterConfig],
        headless: bool = None,
    ):
        """
        Initialize matches scraper.

        Args:
            config: AllCompetitionsConfig or FilterConfig with UUIDs
            headless: Run Chrome in headless mode (default: from config)
        """
        super().__init__(headless=headless, log_name="matches")
        self.config = config
        self.is_all_competitions = isinstance(config, AllCompetitionsConfig)
        self.processed_matches: set = set()  # For deduplication

    def _normalize_competition_name(self, name: str) -> str:
        """
        Normalize competition name for consistent counting.

        Handles:
        - Different apostrophe characters (', ', ʼ)
        - Internal whitespace/newlines
        - Leading/trailing whitespace
        """
        if not name:
            return "Unknown"

        # Normalize various apostrophe characters to standard single quote
        name = name.replace("'", "'").replace("'", "'").replace("ʼ", "'")

        # Normalize whitespace (collapse multiple spaces/newlines to single space)
        name = ' '.join(name.split())

        return name.strip() or "Unknown"

    def _build_base_url(self) -> str:
        """Build base fixtures URL (no match-day)."""
        if self.is_all_competitions:
            # All Competitions: empty competition param
            return (
                f"{COMPETITIONS_URL}/{self.config.path}/fixtures"
                f"?season={self.config.season_uuid}"
                f"&competition-group={self.config.competition_group_uuid}"
                f"&competition="
            )
        else:
            # Specific competition
            return (
                f"{COMPETITIONS_URL}/{self.config.path}/fixtures"
                f"?season={self.config.season_uuid}"
                f"&competition-group={self.config.competition_group_uuid}"
                f"&competition={self.config.competition_uuid}"
            )

    def _build_url_for_date(self, match_day: str) -> str:
        """Build URL for specific match day."""
        if self.is_all_competitions:
            return (
                f"{COMPETITIONS_URL}/{self.config.path}/fixtures"
                f"?season={self.config.season_uuid}"
                f"&competition-group={self.config.competition_group_uuid}"
                f"&competition="
                f"&match-day={match_day}"
            )
        else:
            return (
                f"{COMPETITIONS_URL}/{self.config.path}/fixtures"
                f"?season={self.config.season_uuid}"
                f"&competition-group={self.config.competition_group_uuid}"
                f"&competition={self.config.competition_uuid}"
                f"&match-day={match_day}"
            )

    def scrape(
        self,
        limit: int = 0,
        limit_per_competition: int = 0,
        on_batch_complete: Optional[Callable[[List[MatchRow]], None]] = None,
    ) -> List[MatchRow]:
        """
        Scrape all matches across all available dates.

        Args:
            limit: Maximum total number of matches to scrape (0 = no limit)
            limit_per_competition: Maximum matches per competition (0 = no limit)
            on_batch_complete: Optional callback called after each date with new matches.
                               Signature: callback(matches: List[MatchRow]) -> None
                               Use this for incremental saving.

        Returns:
            List of MatchRow objects (both played and upcoming)
        """
        self.limit_per_competition = limit_per_competition
        self.competition_match_counts: Dict[str, int] = {}  # Track matches per competition

        mode = "All Competitions" if self.is_all_competitions else self.config.competition_name
        self.logger.info(f"Scraping matches: {self.config.competition_group_name} {self.config.season_name} ({mode})")
        if limit_per_competition > 0:
            self.logger.info(f"Limiting to {limit_per_competition} matches per competition")

        # 1. Navigate to base fixtures page to get available dates
        base_url = self._build_base_url()
        self.navigate_to(base_url)

        # 2. Get all available match days from date picker
        available_dates = self._get_available_dates()
        self.logger.info(f"Found {len(available_dates)} match days")

        all_matches = []

        # 3. Iterate through each date and extract matches
        for date_info in available_dates:
            match_day = date_info["date"]  # YYYY-MM-DD
            self.logger.info(f"Scraping matches for {match_day}")

            url = self._build_url_for_date(match_day)
            self.navigate_to(url)

            matches = self._extract_matches_from_page(match_day)
            all_matches.extend(matches)

            self.logger.info(f"Found {len(matches)} matches for {match_day} (total: {len(all_matches)})")

            # Incremental save callback
            if matches and on_batch_complete:
                on_batch_complete(matches)

            # Stop early if limit reached
            if limit > 0 and len(all_matches) >= limit:
                all_matches = all_matches[:limit]
                self.logger.info(f"Reached limit of {limit} matches, stopping early")
                break

        self.logger.info(f"Total unique matches: {len(all_matches)}")
        return all_matches

    def _get_available_dates(self) -> List[Dict[str, str]]:
        """Extract available dates from date picker timeline."""
        dates = []

        # Try to find date picker items
        selectors_to_try = [
            SELECTORS.date_item,  # .c-date-picker-timeline__item
            ".c-date-picker-timeline__item",
        ]

        date_items = None
        for selector in selectors_to_try:
            try:
                date_items = self.wait_for_elements(selector, timeout=15)
                if date_items:
                    self.logger.info(f"Found date picker with {len(date_items)} items")
                    break
            except Exception:
                continue

        if not date_items:
            self.logger.warning("No date picker found, using current date only")
            return [{"date": date.today().isoformat()}]

        for item in date_items:
            try:
                # Look for time element with datetime attribute
                time_elem = item.find_element(By.CSS_SELECTOR, "time")
                datetime_attr = time_elem.get_attribute("datetime")
                if datetime_attr:
                    # Format: 2026-01-25T00:00:00 -> 2026-01-25
                    date_str = datetime_attr.split("T")[0]
                    dates.append({"date": date_str})
            except Exception:
                continue

        return dates

    def _extract_matches_from_page(self, match_day: str) -> List[MatchRow]:
        """Extract all matches from current page."""
        matches = []

        if self.is_all_competitions:
            # All Competitions mode: need to find ribbons and associate matches
            matches = self._extract_matches_with_ribbons(match_day)
        else:
            # Specific competition mode: all matches belong to same competition
            matches = self._extract_matches_single_competition(match_day)

        return matches

    def _extract_matches_with_ribbons(self, match_day: str) -> List[MatchRow]:
        """
        Extract matches from 'All Competitions' page with ribbon headers.

        Page structure:
        - Ribbon (competition name)
        - Match cards for that competition
        - Ribbon (next competition)
        - Match cards for that competition
        - etc.
        """
        matches = []

        # Temporarily disable implicit wait for faster extraction
        # (we're finding elements within known containers, no need to wait)
        # This speeds up extraction from ~6 minutes to ~10 seconds
        from extract.config import SELENIUM_CONFIG
        self.driver.implicitly_wait(0)

        try:
            # Get all ribbon and match container elements in DOM order
            try:
                # Find the fixtures list container
                fixtures_list = self.driver.find_element(By.CSS_SELECTOR, ".js-fixtures-list")

                # Get all direct children that are either ribbons or match containers
                all_elements = fixtures_list.find_elements(
                    By.CSS_SELECTOR,
                    ".c-ribbon, .c-match-detail-card__container"
                )
            except Exception as e:
                self.logger.warning(f"Could not find fixtures list: {e}")
                return []

            current_competition = "Unknown"

            for element in all_elements:
                class_attr = element.get_attribute("class") or ""

                if "c-ribbon" in class_attr:
                    # This is a ribbon - extract and normalize competition name
                    raw_name = None
                    try:
                        link = element.find_element(By.CSS_SELECTOR, SELECTORS.ribbon_link)
                        raw_name = link.text
                    except Exception:
                        try:
                            title = element.find_element(By.CSS_SELECTOR, SELECTORS.ribbon_title)
                            raw_name = title.text
                        except Exception:
                            pass

                    if raw_name:
                        current_competition = self._normalize_competition_name(raw_name)
                        self.logger.info(f"[RIBBON] Competition: '{current_competition}'")

                elif "c-match-detail-card__container" in class_attr:
                    # This is a match container - parse it with current competition
                    # Check limit_per_competition before parsing
                    if self.limit_per_competition > 0:
                        current_count = self.competition_match_counts.get(current_competition, 0)
                        if current_count >= self.limit_per_competition:
                            self.logger.debug(f"[LIMIT] Skipping match for '{current_competition}' (count={current_count}, limit={self.limit_per_competition})")
                            continue  # Skip this match, already at limit for this competition

                    try:
                        match_data = self._parse_match(element, match_day, current_competition)
                        if match_data:
                            match_id = self._create_match_id(match_data)
                            if match_id not in self.processed_matches:
                                self.processed_matches.add(match_id)
                                matches.append(match_data)
                                # Track count per competition
                                new_count = self.competition_match_counts.get(current_competition, 0) + 1
                                self.competition_match_counts[current_competition] = new_count
                                self.logger.info(f"[MATCH] Added match for '{current_competition}' (count={new_count}/{self.limit_per_competition if self.limit_per_competition > 0 else 'unlimited'})")
                    except Exception as e:
                        self.logger.warning(f"Failed to parse match: {e}")
                        continue

            return matches
        finally:
            # Restore implicit wait
            self.driver.implicitly_wait(SELENIUM_CONFIG.implicit_wait)

    def _extract_matches_single_competition(self, match_day: str) -> List[MatchRow]:
        """Extract matches when scraping a specific competition (legacy mode)."""
        matches = []

        # Try multiple selectors for match containers
        selectors_to_try = [
            SELECTORS.match_container,  # .c-match-detail-card__container
            SELECTORS.match_card,       # .c-fixture
        ]

        containers = None
        for selector in selectors_to_try:
            try:
                containers = self.wait_for_elements(selector, timeout=10)
                if containers:
                    self.logger.debug(f"Found {len(containers)} containers with {selector}")
                    break
            except Exception:
                continue

        if not containers:
            self.logger.warning(f"No match containers found for {match_day}")
            return []

        # Competition name comes from config in this mode
        competition_name = self.config.competition_name

        # Temporarily disable implicit wait for faster extraction
        from extract.config import SELENIUM_CONFIG
        self.driver.implicitly_wait(0)

        try:
            for container in containers:
                try:
                    match_data = self._parse_match(container, match_day, competition_name)
                    if match_data:
                        # Deduplication check
                        match_id = self._create_match_id(match_data)
                        if match_id not in self.processed_matches:
                            self.processed_matches.add(match_id)
                            matches.append(match_data)
                except Exception as e:
                    self.logger.warning(f"Failed to parse match: {e}")
                    continue
        finally:
            # Restore implicit wait
            self.driver.implicitly_wait(SELENIUM_CONFIG.implicit_wait)

        return matches

    def _parse_match(
        self,
        container: WebElement,
        match_day: str,
        competition_name: str,
    ) -> Optional[MatchRow]:
        """Parse a single match container into MatchRow."""
        # Extract teams
        home_team = self._extract_team(container, SELECTORS.home_team_badge)
        away_team = self._extract_team(container, SELECTORS.away_team_badge)

        if not home_team or not away_team:
            return None

        # Extract scores (None if not played yet)
        home_score, away_score = self._extract_scores(container)

        # Extract match time
        match_time = self._extract_match_time(container)

        # Extract venue
        venue = self._extract_venue(container)

        # Extract match URL
        match_url = self._extract_match_url(container)

        # Derive phase from competition name (only Premier Division has phases)
        if "Phase 2" in competition_name:
            phase = "Phase 2"
        elif "Premier" in competition_name:
            phase = "Phase 1"
        else:
            phase = ""  # Division 1, Conference, etc. don't have phases

        return MatchRow(
            season=self.config.season_name,
            phase=phase,
            competition_group=self.config.competition_group_name,
            competition=competition_name,
            match_date=match_day,
            match_time=match_time or "",
            home_team=home_team,
            away_team=away_team,
            home_score=home_score,
            away_score=away_score,
            venue=venue or "",
            match_url=match_url or "",
            scraped_at=datetime.now().isoformat(timespec="seconds"),
        )

    def _extract_team(self, container: WebElement, badge_selector: str) -> Optional[str]:
        """Extract team name from badge element."""
        try:
            badge = container.find_element(By.CSS_SELECTOR, badge_selector)
            team_elem = badge.find_element(By.CSS_SELECTOR, SELECTORS.team_name)
            return team_elem.text.strip()
        except Exception:
            return None

    def _extract_scores(self, container: WebElement) -> Tuple[Optional[int], Optional[int]]:
        """Extract scores from container. Returns (None, None) if match not played."""
        try:
            score_board = container.find_element(By.CSS_SELECTOR, SELECTORS.score_board)
            score_items = score_board.find_elements(By.CSS_SELECTOR, SELECTORS.score_item)

            if len(score_items) >= 2:
                home_score = self._safe_int(score_items[0].text.strip())
                away_score = self._safe_int(score_items[1].text.strip())
                return home_score, away_score
        except Exception:
            pass

        return None, None  # Match not played yet

    def _extract_match_time(self, container: WebElement) -> Optional[str]:
        """Extract match time for upcoming fixtures."""
        try:
            time_elem = container.find_element(By.CSS_SELECTOR, SELECTORS.match_time)
            return time_elem.text.strip()
        except Exception:
            return None

    def _extract_venue(self, container: WebElement) -> Optional[str]:
        """Extract venue from container."""
        # Primary selector: .c-fixture__location span (where venue text is)
        try:
            venue_elem = container.find_element(By.CSS_SELECTOR, SELECTORS.venue_text)
            venue_text = venue_elem.text.strip()
            if venue_text:
                return venue_text
        except Exception:
            pass

        # Fallback: try the location div itself
        try:
            venue_elem = container.find_element(By.CSS_SELECTOR, SELECTORS.venue)
            venue_text = venue_elem.text.strip()
            if venue_text:
                return venue_text
        except Exception:
            pass

        return None

    def _extract_match_url(self, container: WebElement) -> Optional[str]:
        """Extract match detail URL from container."""
        try:
            link = container.find_element(By.CSS_SELECTOR, SELECTORS.match_link)
            return link.get_attribute("href")
        except Exception:
            return None

    def _create_match_id(self, match: MatchRow) -> str:
        """Create unique ID for deduplication."""
        return f"{match.season}_{match.match_date}_{match.home_team}_vs_{match.away_team}"

    def _safe_int(self, value: str) -> Optional[int]:
        """Safely convert string to int."""
        if not value:
            return None
        try:
            return int(value)
        except ValueError:
            return None
