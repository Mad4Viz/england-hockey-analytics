# Extract (Python / Selenium)

Scrapes league tables, match results, and goal scorer data from England Hockey using Selenium.

---

## Approach

England Hockey organises results by competition. Scraping each competition individually would mean 40+ page loads per season. Instead, this scraper uses "All Competitions" mode, which returns all competitions on a single page.

| | Individual Competitions | All Competitions |
|---|---|---|
| Page loads | 40+ per season | 8 per season |
| Scope | One competition per page | All competitions per page |
| Speed | Slow (browser open/close per competition) | Fast (one session per gender/season) |

The scraper runs incrementally — only new or updated matches are added on each run, using upsert logic to avoid re-scraping historical data.

---

## Architecture

```
extract/
│
│   # Orchestration — what to scrape
├── __main__.py      # CLI entry point
├── config.py        # URLs, selectors, rate limits
├── competitions.py  # Season/competition UUIDs
├── utils.py         # Logging and CSV writers
├── state.py         # Incremental scrape tracking
│
│   # Scrapers — how to scrape
├── scrapers/
│   ├── base.py          # Browser automation (WebDriver, rate limiting)
│   ├── standings.py     # League table scraper
│   ├── matches.py       # Fixtures/results scraper
│   └── match_events.py  # Goals and cards scraper
```

Two layers: the orchestration layer defines what to scrape and where to save it. The scraper layer handles Chrome automation, page waits, and data extraction.

---

## Output

| File | Description | Key Fields |
|------|-------------|------------|
| [`standings.csv`](../data/sample/standings.csv) | League tables | season, competition, team |
| [`matches.csv`](../data/sample/matches.csv) | Fixtures and results | match_url |
| [`match_events.csv`](../data/sample/match_events.csv) | Goals and cards | match_url, player, minute |

All files include a URL column for fact-checking against the source website.
