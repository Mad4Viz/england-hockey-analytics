"""
England Hockey Analytics - Extract CLI

Run scrapers from the command line:
    python -m extract --type all              # Full run
    python -m extract --type standings        # Just standings
    python -m extract --type matches          # Just matches (fixtures + results)
    python -m extract --type match_events     # Just match events (goals, cards)

Options:
    --headless          Run Chrome without visible window
    --test              Output to Test/ folder
    --limit N           Limit matches scraped (for testing)

Output goes to CSV files. Use 'bq load' to upload to BigQuery (see README).
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any

import csv

from extract.config import SAMPLE_DIR, TEST_DIR, StandingsRow, MatchRow, MatchEventRow
from extract.competitions import build_filter_config, get_all_competitions_configs
from extract.scrapers import StandingsScraper, MatchesScraper, MatchEventsScraper
from extract.state import ScrapeState, is_monday
from extract.utils import setup_logger, CSVWriter, CSVUpsertWriter

# Valid scrape types
SCRAPE_TYPES = ["all", "standings", "matches", "match_events", "matches_and_events"]


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="England Hockey Analytics - Web Scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m extract --type all                    Full scrape (all data)
  python -m extract --type standings              Just league tables
  python -m extract --type matches                Just matches (fixtures + results)
  python -m extract --type standings --headless   Run without browser window
  python -m extract --type all --test --limit 5   Test mode with limited matches
        """,
    )

    parser.add_argument(
        "--type",
        choices=SCRAPE_TYPES,
        default="all",
        help="What to scrape: all, standings, matches, match_events, or matches_and_events",
    )

    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run Chrome in headless mode (no visible browser window)",
    )

    parser.add_argument(
        "--division",
        choices=["womens", "mens", "both"],
        default="womens",
        help="Which division to scrape: womens (default), mens, or both",
    )

    parser.add_argument(
        "--full-refresh",
        action="store_true",
        dest="full_refresh",
        help="Force full refresh (ignore incremental logic, scrape everything)",
    )

    parser.add_argument(
        "--level",
        choices=["premier", "div1", "all"],
        default="premier",
        help="Competition level: premier (default), div1, or all",
    )

    parser.add_argument(
        "--season",
        choices=["current", "prior", "both", "all"],
        default="current",
        help="Season: current (2025-2026), prior (2024-2025), both, or all (4 seasons)",
    )

    parser.add_argument(
        "--test",
        action="store_true",
        help="Test mode: output to data/sample/Test/ folder instead of main folder",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit total number of matches scraped (0 = no limit)",
    )

    parser.add_argument(
        "--limit-per-competition",
        type=int,
        default=0,
        dest="limit_per_competition",
        help="Limit matches per competition (0 = no limit). Useful for testing all divisions.",
    )

    return parser.parse_args()


def get_output_dir(args) -> Path:
    """Get output directory based on --test flag."""
    if args.test:
        TEST_DIR.mkdir(parents=True, exist_ok=True)
        return TEST_DIR
    return SAMPLE_DIR


def get_filters(division: str, level: str = "premier", season: str = "current"):
    """
    Get filter configs based on division, level, and season arguments.

    Used for STANDINGS scraping (needs specific competition URLs).

    Args:
        division: womens, mens, or both
        level: premier (all phases), div1 (north/south), or all
        season: current (2025-2026), prior (2024-2025), both, or all (4 seasons)

    Returns:
        List of FilterConfig objects for scraping
    """
    filters = []

    # Determine which seasons to include
    seasons = []
    if season in ("current", "both", "all"):
        seasons.append("2025-2026")
    if season in ("prior", "both", "all"):
        seasons.append("2024-2025")
    if season == "all":
        seasons.append("2023-2024")
        seasons.append("2022-2023")

    for season_name in seasons:
        if division in ("womens", "both"):
            # Premier Division (all 3 phases)
            if level in ("premier", "all"):
                filters.append(build_filter_config("womens", "premier", season_name))
                filters.append(build_filter_config("womens", "premier_phase2_top6", season_name))
                filters.append(build_filter_config("womens", "premier_phase2_lower6", season_name))
            # Division 1 North and South
            if level in ("div1", "all"):
                filters.append(build_filter_config("womens", "div1_north", season_name))
                filters.append(build_filter_config("womens", "div1_south", season_name))

        if division in ("mens", "both"):
            # Premier Division (all 3 phases)
            if level in ("premier", "all"):
                filters.append(build_filter_config("mens", "premier", season_name))
                filters.append(build_filter_config("mens", "premier_phase2_top6", season_name))
                filters.append(build_filter_config("mens", "premier_phase2_lower6", season_name))
            # Division 1 North and South
            if level in ("div1", "all"):
                filters.append(build_filter_config("mens", "div1_north", season_name))
                filters.append(build_filter_config("mens", "div1_south", season_name))

    return filters


def run_standings_scraper(args, logger) -> Dict[str, Any]:
    """Run the standings scraper using 'All Competitions' mode.

    Uses get_all_competitions_configs() to scrape ALL standings for each
    gender/season combination in one page (much more efficient than
    scraping individual competitions).

    Uses CSVUpsertWriter to merge new data with existing data.
    Saves after each gender/season for fault tolerance.

    Returns:
        Dict with results: completed, failed and row counts
    """
    output_dir = get_output_dir(args)
    output_path = output_dir / "standings.csv"
    completed = []
    failed = []

    # Get AllCompetitionsConfig list (e.g., 8 configs for womens/mens × 4 seasons)
    configs = get_all_competitions_configs(gender=args.division, season=args.season)

    # Initialize upsert writer - loads existing data
    writer = CSVUpsertWriter(
        StandingsRow,
        output_path,
        key_fields=["season", "competition", "team"]
    )
    initial_rows = writer.total_rows
    total_inserted = 0
    total_updated = 0

    logger.info(f"Scraping {len(configs)} gender/season combination(s) using 'All Competitions' mode")

    for config in configs:
        config_name = f"{config.competition_group_name} {config.season_name}"

        try:
            logger.info(f"Scraping all standings: {config_name}")

            with StandingsScraper(config, headless=args.headless) as scraper:
                standings = scraper.scrape()

                if standings:
                    inserted, updated = writer.upsert_rows(standings)
                    total_inserted += inserted
                    total_updated += updated
                    # Save after each gender/season for fault tolerance
                    writer.save()
                    logger.info(f"Saved {config_name}: {inserted} new, {updated} updated")
                else:
                    logger.warning(f"No standings found for {config_name}")

            completed.append({
                "season": config.season_name,
                "competition": f"All ({config.competition_group_name})",
                "rows": inserted + updated if standings else 0
            })

        except Exception as e:
            logger.error(f"Failed {config_name}: {e}")
            failed.append({
                "season": config.season_name,
                "competition": f"All ({config.competition_group_name})",
                "error": str(e)
            })
            continue  # Move to next config

    # Log summary
    logger.info(f"Upsert totals: {total_inserted} inserted, {total_updated} updated")
    logger.info(f"Total rows in {output_path}: {writer.total_rows} (was {initial_rows})")
    if failed:
        logger.warning(f"Failed: {[f['competition'] for f in failed]}")

    return {
        "data_type": "standings",
        "completed": completed,
        "failed": failed,
        "total_rows": writer.total_rows,
    }


def run_matches_scraper(args, logger) -> Dict[str, Any]:
    """Run the matches scraper using 'All Competitions' mode.

    Uses get_all_competitions_configs() to scrape ALL matches for each
    gender/season combination in one page (much more efficient than
    scraping individual competitions).

    Uses CSVUpsertWriter to merge new data with existing data.
    Saves after each gender/season for fault tolerance.

    Returns:
        Dict with results: completed, failed and row counts
    """
    state = ScrapeState()
    output_dir = get_output_dir(args)
    output_path = output_dir / "matches.csv"
    completed = []
    failed = []

    # Get AllCompetitionsConfig list (e.g., 4 configs for womens/mens × 2 seasons)
    configs = get_all_competitions_configs(gender=args.division, season=args.season)

    # Initialize upsert writer - loads existing data
    writer = CSVUpsertWriter(
        MatchRow,
        output_path,
        key_fields=["match_url"]  # URL is unique identifier for each match
    )
    initial_rows = writer.total_rows
    total_inserted = 0
    total_updated = 0

    logger.info(f"Scraping {len(configs)} gender/season combination(s) using 'All Competitions' mode")

    for config in configs:
        config_name = f"{config.competition_group_name} {config.season_name}"

        try:
            logger.info(f"Scraping all matches: {config_name}")

            # Incremental save callback - saves to CSV after each date
            def on_batch_complete(batch_matches):
                nonlocal total_inserted, total_updated
                if batch_matches:
                    inserted, updated = writer.upsert_rows(batch_matches)
                    total_inserted += inserted
                    total_updated += updated
                    writer.save()
                    logger.info(f"[SAVED] {inserted} new, {updated} updated (total in file: {writer.total_rows})")

                    # Mark completed matches in state
                    for match in batch_matches:
                        has_result = match.home_score is not None and match.away_score is not None
                        if has_result:
                            match_id = f"{match.season}_{match.match_date}_{match.home_team}_vs_{match.away_team}"
                            state.mark_match_complete(match_id)

            config_rows_before = writer.total_rows

            with MatchesScraper(config, headless=args.headless) as scraper:
                matches = scraper.scrape(
                    limit=args.limit,
                    limit_per_competition=args.limit_per_competition,
                    on_batch_complete=on_batch_complete,
                )

                if not matches:
                    logger.warning(f"No matches found for {config_name}")

            config_rows_added = writer.total_rows - config_rows_before
            completed.append({
                "season": config.season_name,
                "competition": f"All ({config.competition_group_name})",
                "rows": config_rows_added
            })

        except Exception as e:
            logger.error(f"Failed {config_name}: {e}")
            failed.append({
                "season": config.season_name,
                "competition": f"All ({config.competition_group_name})",
                "error": str(e)
            })
            continue  # Move to next config

    # Log summary
    logger.info(f"Upsert totals: {total_inserted} inserted, {total_updated} updated")
    logger.info(f"Total rows in {output_path}: {writer.total_rows} (was {initial_rows})")
    if failed:
        logger.warning(f"Failed: {[f['competition'] for f in failed]}")

    return {
        "data_type": "matches",
        "completed": completed,
        "failed": failed,
        "total_rows": writer.total_rows,
    }


def run_match_events_scraper(args, logger) -> Dict[str, Any]:
    """Run the match events scraper to extract goals and cards with upsert logic.

    Reads completed match URLs from matches.csv and scrapes event details
    from each match page. Uses CSVUpsertWriter to merge with existing data.

    Returns:
        Dict with results: status, matches scraped, total events
    """
    output_dir = get_output_dir(args)
    matches_path = output_dir / "matches.csv"
    output_path = output_dir / "match_events.csv"

    # Read match URLs from matches.csv
    if not matches_path.exists():
        logger.error(f"matches.csv not found at {matches_path}")
        logger.info("Run --type matches first to generate match URLs")
        return {"data_type": "match_events", "status": "skipped", "reason": "matches.csv not found", "total_rows": 0}

    # Get URLs and seasons of completed matches (those with results available)
    match_info = []
    with open(matches_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # A match is complete if scores exist (even 0-0 has results)
            home_score = row.get("home_score", "")
            away_score = row.get("away_score", "")
            has_result = home_score != "" and away_score != ""

            if has_result:
                url = row.get("match_url")
                season = row.get("season", "")
                if url:
                    match_info.append({"url": url, "season": season})

    if not match_info:
        logger.warning("No completed matches found in matches.csv")
        return {"data_type": "match_events", "status": "skipped", "reason": "no completed matches", "total_rows": 0}

    # Apply --limit if specified
    if args.limit > 0:
        match_info = match_info[:args.limit]
        logger.info(f"Limiting to {args.limit} matches (--limit flag)")

    logger.info(f"Found {len(match_info)} completed matches to scrape events from")

    # Initialize upsert writer - loads existing data
    writer = CSVUpsertWriter(
        MatchEventRow,
        output_path,
        key_fields=["match_url", "player_name", "minute", "event_type"]
    )
    initial_rows = writer.total_rows
    total_inserted = 0
    total_updated = 0

    # Incremental save callback - saves to CSV after each match
    def on_match_complete(match_events):
        nonlocal total_inserted, total_updated
        if match_events:
            inserted, updated = writer.upsert_rows(match_events)
            total_inserted += inserted
            total_updated += updated
            writer.save()
            logger.info(f"[SAVED] {inserted} new, {updated} updated (total in file: {writer.total_rows})")

    # Scrape events with incremental saving
    with MatchEventsScraper(headless=args.headless) as scraper:
        events = scraper.scrape_matches(match_info, on_match_complete=on_match_complete)

    # Final summary
    if events:
        logger.info(f"Upsert totals: {total_inserted} new, {total_updated} updated")
        logger.info(f"Total rows in {output_path}: {writer.total_rows} (was {initial_rows})")
    else:
        logger.warning("No events found")

    return {
        "data_type": "match_events",
        "status": "complete",
        "matches_scraped": len(match_info),
        "total_rows": writer.total_rows,
    }


def write_scrape_summary(results: List[Dict[str, Any]], args, logger) -> None:
    """Write scrape progress summary to CSV with upsert logic.

    Preserves historical data and only updates rows for competitions
    that were scraped in this run. Key: (data_type, season, competition).

    Args:
        results: List of result dicts from each scraper
        args: Parsed command line arguments
        logger: Logger instance
    """
    output_dir = get_output_dir(args)
    output_path = output_dir / "scrape_progress_summary.csv"
    timestamp = datetime.now().isoformat(timespec="seconds")
    fieldnames = ["data_type", "season", "gender", "competition", "phase", "status", "row_count", "notes", "scraped_at"]

    # Load existing data keyed by (data_type, season, competition)
    existing_data: Dict[tuple, dict] = {}
    if output_path.exists():
        with open(output_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = (row.get("data_type", ""), row.get("season", ""), row.get("competition", ""))
                existing_data[key] = row

    # Process new results and upsert
    inserted = 0
    updated = 0

    for result in results:
        data_type = result.get("data_type", "")

        if data_type == "match_events":
            # Match events is a single entry
            status = result.get("status", "unknown")
            key = ("match_events", "All", "All")
            new_row = {
                "data_type": "match_events",
                "season": "All",
                "gender": "All",
                "competition": "All",
                "phase": "",
                "status": status,
                "row_count": result.get("total_rows", 0),
                "notes": f"Scraped {result.get('matches_scraped', 0)} matches" if status == "complete" else result.get("reason", ""),
                "scraped_at": timestamp,
            }
            if key in existing_data:
                updated += 1
            else:
                inserted += 1
            existing_data[key] = new_row
        else:
            # Standings and matches have per-competition results
            for comp in result.get("completed", []):
                comp_name = comp.get("competition", "")
                season = comp.get("season", "")
                gender = "Mens" if "Mens" in comp_name or "Open" in comp_name else "Womens"
                phase = "Phase 2" if "Phase 2" in comp_name else ("Phase 1" if "Premier" in comp_name else "")

                key = (data_type, season, comp_name)
                new_row = {
                    "data_type": data_type,
                    "season": season,
                    "gender": gender,
                    "competition": comp_name,
                    "phase": phase,
                    "status": "complete",
                    "row_count": comp.get("rows", 0),
                    "notes": "",
                    "scraped_at": timestamp,
                }
                if key in existing_data:
                    updated += 1
                else:
                    inserted += 1
                existing_data[key] = new_row

            for comp in result.get("failed", []):
                comp_name = comp.get("competition", "")
                season = comp.get("season", "")
                gender = "Mens" if "Mens" in comp_name or "Open" in comp_name else "Womens"
                phase = "Phase 2" if "Phase 2" in comp_name else ("Phase 1" if "Premier" in comp_name else "")

                key = (data_type, season, comp_name)
                new_row = {
                    "data_type": data_type,
                    "season": season,
                    "gender": gender,
                    "competition": comp_name,
                    "phase": phase,
                    "status": "failed",
                    "row_count": 0,
                    "notes": comp.get("error", "")[:100],  # Truncate error message
                    "scraped_at": timestamp,
                }
                if key in existing_data:
                    updated += 1
                else:
                    inserted += 1
                existing_data[key] = new_row

    # Write all data back to CSV
    if existing_data:
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in existing_data.values():
                writer.writerow(row)

        logger.info(f"Scrape summary updated: {inserted} new, {updated} updated (total: {len(existing_data)} entries)")


def main() -> int:
    """Main entry point for the scraper."""
    args = parse_args()
    logger = setup_logger()

    logger.info("=" * 60)
    logger.info("England Hockey Analytics - Extract")
    logger.info("=" * 60)
    logger.info(f"Scrape type: {args.type}")
    logger.info(f"Division: {args.division}")
    logger.info(f"Level: {args.level} (ignored - using All Competitions mode)")
    logger.info(f"Season: {args.season}")
    logger.info(f"Headless: {args.headless}")
    logger.info(f"Full refresh: {args.full_refresh}")
    logger.info(f"Test mode: {args.test}")
    if args.limit > 0:
        logger.info(f"Match limit (total): {args.limit}")
    if args.limit_per_competition > 0:
        logger.info(f"Match limit (per competition): {args.limit_per_competition}")
    logger.info(f"Output directory: {get_output_dir(args)}")
    logger.info(f"Is Monday (auto-refresh day): {is_monday()}")
    logger.info("Standings and Matches use 'All Competitions' mode (faster)")
    logger.info("=" * 60)

    results = []

    try:
        # Run standings scraper
        if args.type in ("all", "standings"):
            standings_result = run_standings_scraper(args, logger)
            results.append(standings_result)

        # Run matches scraper
        if args.type in ("all", "matches", "matches_and_events"):
            matches_result = run_matches_scraper(args, logger)
            results.append(matches_result)

        # Run match events scraper
        if args.type in ("all", "match_events", "matches_and_events"):
            events_result = run_match_events_scraper(args, logger)
            results.append(events_result)

        # Write scrape summary
        if results:
            write_scrape_summary(results, args, logger)

        logger.info("=" * 60)
        logger.info("Scrape complete")
        logger.info("=" * 60)
        return 0

    except Exception as e:
        logger.error(f"Scrape failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
