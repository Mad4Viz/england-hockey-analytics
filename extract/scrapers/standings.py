"""
England Hockey Analytics - Standings Scraper
Scrapes league standings table from the England Hockey website.

Supports two modes:
1. AllCompetitionsConfig: Scrapes ALL competitions for a gender/season in one page
2. FilterConfig: Scrapes a specific competition (legacy mode)
"""

from datetime import datetime
from typing import List, Union

from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement

from extract.scrapers.base import BaseScraper
from extract.config import (
    COMPETITIONS_URL,
    STANDINGS_URL_PATTERN,
    SELECTORS,
    StandingsRow,
    SAMPLE_DIR,
)
from extract.competitions import FilterConfig, AllCompetitionsConfig


class StandingsScraper(BaseScraper):
    """
    Scrapes league standings table.

    Usage (All Competitions - recommended):
        from extract.competitions import build_all_competitions_config

        config = build_all_competitions_config("womens", "2024-2025")
        with StandingsScraper(config) as scraper:
            standings = scraper.scrape()

    Usage (Specific Competition - legacy):
        from extract.competitions import WOMENS_PREMIER_FILTER

        with StandingsScraper(WOMENS_PREMIER_FILTER) as scraper:
            standings = scraper.scrape()
    """

    def __init__(
        self,
        config: Union[AllCompetitionsConfig, FilterConfig],
        headless: bool = None,
    ) -> None:
        """
        Initialize standings scraper.

        Args:
            config: AllCompetitionsConfig or FilterConfig with UUIDs
            headless: Run Chrome in headless mode (default: from config)
        """
        super().__init__(headless=headless, log_name="standings")
        self.config = config
        self.is_all_competitions = isinstance(config, AllCompetitionsConfig)

    def _build_url(self) -> str:
        """Build standings URL from config."""
        if self.is_all_competitions:
            # All Competitions: empty competition param
            return (
                f"{COMPETITIONS_URL}/{self.config.path}/table"
                f"?season={self.config.season_uuid}"
                f"&competition-group={self.config.competition_group_uuid}"
                f"&competition="
            )
        else:
            # Specific competition (legacy)
            return STANDINGS_URL_PATTERN.format(
                competitions_url=COMPETITIONS_URL,
                path=self.config.path,
                season_uuid=self.config.season_uuid,
                comp_group_uuid=self.config.competition_group_uuid,
                competition_uuid=self.config.competition_uuid,
            )

    def scrape(self) -> List[StandingsRow]:
        """
        Navigate to standings page and extract table data.

        Returns:
            List of StandingsRow objects with team standings
        """
        url = self._build_url()
        self.current_url = url  # Store for use in _parse_row()

        if self.is_all_competitions:
            mode = "All Competitions"
            self.logger.info(f"Scraping standings: {self.config.competition_group_name} {self.config.season_name} ({mode})")
        else:
            self.logger.info(f"Scraping standings for: {self.config.competition_name}")

        self.navigate_to(url)

        if self.is_all_competitions:
            return self._extract_standings_with_ribbons()
        else:
            return self._extract_standings_single_competition()

    def _extract_standings_with_ribbons(self) -> List[StandingsRow]:
        """
        Extract standings from 'All Competitions' page with ribbon headers.

        Page structure:
        - .js-competition-table container
          - .c-ribbon (competition name)
          - .c-table-container > table (standings for that competition)
          - .c-ribbon (next competition)
          - .c-table-container > table
          - etc.
        """
        standings = []

        try:
            # Find the competition table container
            container = self.driver.find_element(By.CSS_SELECTOR, ".js-competition-table")

            # Get all ribbon and table-container elements in DOM order
            all_elements = container.find_elements(
                By.CSS_SELECTOR,
                ".c-ribbon, .c-table-container"
            )
        except Exception as e:
            self.logger.warning(f"Could not find competition table container: {e}")
            return []

        current_competition = "Unknown"

        for element in all_elements:
            class_attr = element.get_attribute("class") or ""

            if "c-ribbon" in class_attr:
                # This is a ribbon - extract competition name
                try:
                    title = element.find_element(By.CSS_SELECTOR, SELECTORS.ribbon_title)
                    current_competition = title.text.strip()
                    self.logger.debug(f"Competition: {current_competition}")
                except Exception:
                    pass

            elif "c-table-container" in class_attr:
                # This is a table container - parse all rows with current competition
                try:
                    rows = element.find_elements(By.CSS_SELECTOR, "table tbody tr")
                    self.logger.info(f"Found {len(rows)} teams in {current_competition}")

                    for row in rows:
                        try:
                            parsed = self._parse_row(row, current_competition)
                            standings.append(parsed)
                            self.logger.debug(f"Parsed: {parsed.position}. {parsed.team} - {parsed.points} pts")
                        except Exception as e:
                            self.logger.warning(f"Failed to parse row: {e}")
                            continue
                except Exception as e:
                    self.logger.warning(f"Failed to parse table for {current_competition}: {e}")
                    continue

        self.logger.info(f"Total standings rows: {len(standings)}")
        return standings

    def _extract_standings_single_competition(self) -> List[StandingsRow]:
        """Extract standings when scraping a specific competition (legacy mode)."""
        # Try multiple selectors for the table rows
        rows = None
        selectors_to_try = [
            SELECTORS.standings_row,  # .c-table--standings tbody tr
            "table tbody tr",         # Generic fallback
            ".c-table tbody tr",      # Alternative class
        ]

        for selector in selectors_to_try:
            try:
                self.logger.info(f"Trying selector: {selector}")
                rows = self.wait_for_elements(selector, timeout=15)
                if rows:
                    self.logger.info(f"Found {len(rows)} rows with selector: {selector}")
                    break
            except Exception as e:
                self.logger.debug(f"Selector {selector} failed: {e}")
                continue

        if not rows:
            # Check if page is 404 or empty (Phase 2 may not exist yet)
            page_source = self.get_page_source()
            if "Page Not Found" in page_source or "404" in page_source:
                self.logger.warning(
                    f"Page not found for {self.config.competition_name} - "
                    "Phase may not have started yet. Skipping."
                )
                return []

            # Unknown error - save debug page
            self.logger.error("No table rows found. Saving page source for debugging.")
            debug_path = SAMPLE_DIR / "debug_standings_page.html"
            with open(debug_path, "w") as f:
                f.write(page_source)
            self.logger.info(f"Page source saved to {debug_path}")
            return []  # Return empty instead of crashing

        self.logger.info(f"Found {len(rows)} teams in standings table")

        standings = []
        competition_name = self.config.competition_name
        for row in rows:
            try:
                parsed = self._parse_row(row, competition_name)
                standings.append(parsed)
                self.logger.debug(f"Parsed: {parsed.position}. {parsed.team} - {parsed.points} pts")
            except Exception as e:
                self.logger.warning(f"Failed to parse row: {e}")
                continue

        self.logger.info(f"Successfully parsed {len(standings)} standings rows")
        return standings

    def _parse_row(self, row: WebElement, competition_name: str) -> StandingsRow:
        """
        Parse a single table row into StandingsRow.

        Args:
            row: WebElement representing a table row
            competition_name: Name of the competition this row belongs to

        Returns:
            StandingsRow with parsed data
        """
        # Derive phase from competition name (only Premier Division has phases)
        if "Phase 2" in competition_name:
            phase = "Phase 2"
        elif "Premier" in competition_name:
            phase = "Phase 1"
        else:
            phase = ""  # Division 1, Conference, etc. don't have phases

        return StandingsRow(
            season=self.config.season_name,
            phase=phase,
            competition_group=self.config.competition_group_name,
            competition=competition_name,
            position=self._safe_int(self.get_element_text(row, SELECTORS.standings_position)),
            team=self.get_element_text(row, SELECTORS.standings_team),
            played=self._safe_int(self.get_element_text(row, SELECTORS.standings_played)),
            won=self._safe_int(self.get_element_text(row, SELECTORS.standings_won)),
            drawn=self._safe_int(self.get_element_text(row, SELECTORS.standings_drawn)),
            lost=self._safe_int(self.get_element_text(row, SELECTORS.standings_lost)),
            goals_for=self._safe_int(self.get_element_text(row, SELECTORS.standings_goals_for)),
            goals_against=self._safe_int(self.get_element_text(row, SELECTORS.standings_goals_against)),
            goal_difference=self._safe_int(self.get_element_text(row, SELECTORS.standings_goal_diff)),
            points=self._safe_int(self.get_element_text(row, SELECTORS.standings_points)),
            standings_url=self.current_url,
            scraped_at=datetime.now().isoformat(timespec="seconds"),
        )

    def _safe_int(self, value: str) -> int:
        """
        Safely convert string to int, handling empty strings and invalid values.

        Args:
            value: String value to convert

        Returns:
            Integer value, or 0 if conversion fails
        """
        if not value:
            return 0
        try:
            return int(value)
        except ValueError:
            self.logger.warning(f"Could not convert '{value}' to int, using 0")
            return 0
