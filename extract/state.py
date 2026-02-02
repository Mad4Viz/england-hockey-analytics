"""
England Hockey Analytics - Incremental Scraping State

Weekly refresh logic:
- Monday: Full refresh (matches played on weekend, website updated)
- Tue-Sun: Incremental (skip what already exists)
- --full-refresh: Override to force full refresh any day
"""

import json
from datetime import date
from pathlib import Path
from typing import Set

from extract.config import DATA_DIR


def is_monday() -> bool:
    """Check if today is Monday (refresh day)."""
    return date.today().weekday() == 0  # Monday = 0


def should_full_refresh(force_refresh: bool = False) -> bool:
    """Determine if we should do a full refresh."""
    return force_refresh or is_monday()


class ScrapeState:
    """
    Track what has been scraped for incremental loading.

    State is persisted to a JSON file in the data directory.
    """

    def __init__(self, state_file: Path = None):
        """
        Initialize state tracker.

        Args:
            state_file: Path to state file (default: data/scrape_state.json)
        """
        self.state_file = state_file or DATA_DIR / "scrape_state.json"
        self.state = self._load_state()

    def _load_state(self) -> dict:
        """Load state from file or return empty state."""
        if self.state_file.exists():
            with open(self.state_file) as f:
                data = json.load(f)
                # Convert matches list back to set for efficient lookups
                data["completed_matches"] = set(data.get("completed_matches", []))
                return data
        return {
            "standings": {},  # {competition: last_snapshot_date}
            "completed_matches": set(),  # Match IDs with final scores
        }

    def _save_state(self) -> None:
        """Save state to file."""
        # Ensure parent directory exists
        self.state_file.parent.mkdir(parents=True, exist_ok=True)

        state_to_save = {
            "standings": self.state["standings"],
            "completed_matches": list(self.state["completed_matches"]),
        }
        with open(self.state_file, "w") as f:
            json.dump(state_to_save, f, indent=2)

    def clear(self) -> None:
        """Clear all state (for full refresh)."""
        self.state = {"standings": {}, "completed_matches": set()}
        self._save_state()

    # -------------------------------------------------------------------------
    # Standings methods
    # -------------------------------------------------------------------------

    def should_scrape_standings(self, competition: str, full_refresh: bool = False) -> bool:
        """
        Check if we should scrape standings for this competition.

        Args:
            competition: Competition name
            full_refresh: Force full refresh flag

        Returns:
            True if should scrape, False to skip
        """
        if should_full_refresh(full_refresh):
            return True
        today = date.today().isoformat()
        return self.state["standings"].get(competition) != today

    def mark_standings_scraped(self, competition: str) -> None:
        """
        Mark standings as scraped for today.

        Args:
            competition: Competition name
        """
        self.state["standings"][competition] = date.today().isoformat()
        self._save_state()

    # -------------------------------------------------------------------------
    # Matches methods
    # -------------------------------------------------------------------------

    def is_match_complete(self, match_id: str, full_refresh: bool = False) -> bool:
        """
        Check if match already has final scores recorded.

        Args:
            match_id: Unique match identifier
            full_refresh: Force full refresh flag

        Returns:
            True if match is complete (skip it), False to re-scrape
        """
        if should_full_refresh(full_refresh):
            return False  # Re-scrape everything on full refresh
        return match_id in self.state["completed_matches"]

    def mark_match_complete(self, match_id: str) -> None:
        """
        Mark match as having final scores (don't re-scrape).

        Args:
            match_id: Unique match identifier
        """
        self.state["completed_matches"].add(match_id)
        self._save_state()

    def get_stats(self) -> dict:
        """Get summary statistics of current state."""
        return {
            "standings_tracked": len(self.state["standings"]),
            "completed_matches": len(self.state["completed_matches"]),
            "is_monday": is_monday(),
        }
