# Extract Module

This module handles web scraping for England Hockey league data using "All Competitions" mode for efficiency.

## Quick Start

```bash
# Full scrape (all 4 seasons, both genders)
python -m extract --type all --division both --season all --headless

# Just standings
python -m extract --type standings --division both --season all --headless

# Just matches (fixtures + results in one file)
python -m extract --type matches --division both --season all --headless

# Match events (goals, cards)
python -m extract --type match_events --headless

# Test mode (limited data, separate output folder)
python -m extract --type all --division both --season all --test --limit 2 --headless
```

**Options:**
- `--headless` - Run Chrome without visible window
- `--division womens` - Women's competitions (default)
- `--division mens` - Men's competitions
- `--division both` - Both genders
- `--season current` - 2025-2026 season (default)
- `--season prior` - 2024-2025 season
- `--season both` - Current + prior seasons
- `--season all` - All 4 seasons (2022-2026)
- `--level` - (Ignored - "All Competitions" mode gets everything)
- `--limit N` - Limit matches scraped (for testing)
- `--test` - Output to data/sample/Test/ folder
- `--full-refresh` - Ignore incremental logic, scrape everything

Output is always CSV. Use `bq load` commands below to upload to BigQuery.

## All Competitions Mode

Instead of scraping individual competitions one by one (40+ page loads), this scraper uses "All Competitions" mode:

| Old Approach | New Approach |
|--------------|--------------|
| 40+ page loads | 8 page loads |
| One competition per page | All competitions per page |
| Slow (browser open/close per comp) | Fast (one browser session per gender/season) |

**How it works:**
1. Navigate to URL with `competition=` (empty parameter)
2. Page shows ALL competitions for that gender/season
3. Extract competition name from `.c-ribbon__title` elements
4. Parse all tables/matches in one page load

## BigQuery Upload

**Status**: Tables created in `hockey_raw` dataset (Jan 31, 2026)

Upload existing CSVs to BigQuery tables:

```bash
# Upload standings
bq load --source_format=CSV --autodetect \
  england-hockey-analytics:hockey_raw.standings \
  data/sample/standings.csv

# Upload matches
bq load --source_format=CSV --autodetect \
  england-hockey-analytics:hockey_raw.matches \
  data/sample/matches.csv

# Upload match events
bq load --source_format=CSV --autodetect \
  england-hockey-analytics:hockey_raw.match_events \
  data/sample/match_events.csv
```

**Notes:**
- `--autodetect` infers schema from CSV headers
- Tables must exist in `hockey_raw` dataset
- Use this to restore data if BigQuery tables expire (sandbox 60-day limit)

## What's Here

```
extract/
│
│   # ORCHESTRATION LAYER - "What to scrape"
├── __main__.py      # CLI entry point (python -m extract)
├── config.py        # URLs, selectors, rate limits, data structures
├── competitions.py  # Season/competition UUIDs for filters
├── utils.py         # Logging and CSV writers (CSVUpsertWriter)
├── state.py         # Incremental scrape tracking
│
│   # SCRAPER LAYER - "How to scrape" (does the actual Chrome work)
├── scrapers/
│   ├── base.py          # Shared browser automation (WebDriver setup, rate limiting)
│   ├── standings.py     # League table scraper
│   ├── matches.py       # Fixtures/results scraper
│   └── match_events.py  # Goals and cards scraper
│
│   # REFERENCE FILES - HTML samples used during development
├── notes/
│   ├── All_Competitions.txt  # Sample HTML from "All Competitions" fixtures page
│   ├── All_Tables.txt       # Sample HTML from standings page
│   └── URLS_NEEDED.txt      # URL patterns and parameters
│
└── README.md
```

**Two-layer architecture:**
- **Orchestration layer** (top-level `.py` files): Defines what to scrape, where to save, CLI args
- **Scraper layer** (`scrapers/` folder): Contains classes that open Chrome and extract data

## How It Works

### config.py
All settings live in one place so they're easy to change:
- **URLs**: Where to find standings, fixtures, and match details
- **CSS Selectors**: How to locate elements on the page (tables, scores, team names)
- **Rate Limits**: Wait 3-5 seconds between requests to avoid getting blocked
- **Data Structures**: Define what fields we collect (team, score, date, etc.)

### scraper.py
The `BaseScraper` class handles browser automation:
- Opens Chrome (visible for debugging, or headless for production)
- Waits for pages to fully load before extracting data
- Retries automatically if something fails
- Closes the browser properly when done

### utils.py
Helper tools for output:
- **Logger**: Records what happened during scraping (for debugging)
- **CSVWriter**: Saves data to CSV files incrementally as we scrape

## Usage Example

```python
from extract.scraper import BaseScraper

class StandingsScraper(BaseScraper):
    def scrape(self):
        self.navigate_to("https://englandhockey.co.uk/...")
        # Find the standings table
        # Extract team names, points, etc.
        # Save to CSV

# Run the scraper
with StandingsScraper() as scraper:
    scraper.scrape()
```

The `with` statement ensures Chrome closes properly even if an error occurs.

## Output Files

| File | Description | Key Fields |
|------|-------------|------------|
| `standings.csv` | League tables | season, competition, team |
| `matches.csv` | Fixtures and results | match_url |
| `match_events.csv` | Goals and cards | match_url, player, minute |

All files include a URL column for fact-checking against the source website.
