"""
England Hockey Analytics - Competition Configurations
All seasons, competition groups, and competition UUIDs for scraping.

UUIDs extracted from browser DevTools inspection of filter dropdowns on:
https://www.englandhockey.co.uk/competitions

IMPORTANT: Competition group and competition UUIDs are SEASON-SPECIFIC.
Each season has its own set of UUIDs for the same competitions.
"""

from dataclasses import dataclass
from typing import Dict, Final


# =============================================================================
# DATACLASS
# =============================================================================

@dataclass(frozen=True)
class FilterConfig:
    """Configuration for website filter selections (specific competition)."""
    season_name: str
    season_uuid: str
    competition_group_name: str
    competition_group_uuid: str
    competition_name: str
    competition_uuid: str
    path: str  # URL path segment for this competition


@dataclass(frozen=True)
class AllCompetitionsConfig:
    """Configuration for 'All Competitions' view (shows all matches for a gender/season)."""
    season_name: str
    season_uuid: str
    competition_group_name: str  # "womens" or "mens"
    competition_group_uuid: str
    path: str  # URL path segment


# =============================================================================
# SEASONS
# =============================================================================

SEASONS: Final[Dict[str, str]] = {
    "2025-2026": "e9427139-5a57-4841-ab3e-1087bf72678e",
    "2024-2025": "14edd6a1-2d0e-447a-8550-68b42882e46d",
    "2023-2024": "3d87a2df-f97d-47a1-8371-b8e5267c5360",
    "2022-2023": "2f47c1aa-211b-428c-a2a9-56bd35017d86",
}

# Current season for default scraping
CURRENT_SEASON: Final[str] = "2025-2026"
CURRENT_SEASON_UUID: Final[str] = SEASONS[CURRENT_SEASON]


# =============================================================================
# COMPETITION GROUPS (Season-Specific)
# =============================================================================

COMP_GROUPS: Final[Dict[str, Dict[str, Dict[str, str]]]] = {
    "2025-2026": {
        "womens": {
            "name": "EH Adult EHL Womens",
            "uuid": "edd72a6f-8bb4-46ad-80c8-2018f8399e8f",
        },
        "mens": {
            "name": "EH Adult EHL Open/Mens",
            "uuid": "6352e8c0-c2b3-4f7f-ae76-db0bbf8819c0",
        },
    },
    "2024-2025": {
        "womens": {
            "name": "EH Adult EHL Womens",
            "uuid": "5b419601-48a5-42c5-a0b2-d7cf71e641f9",
        },
        "mens": {
            "name": "EH Adult EHL Open/Mens",
            "uuid": "bd254499-a9fd-4c05-8b15-62a61dbb2399",
        },
    },
    "2023-2024": {
        "womens": {
            "name": "EH Adult EHL Womens",
            "uuid": "43402099-b478-4080-a6d8-89736a1473e6",
        },
        "mens": {
            "name": "EH Adult EHL Open/Mens",
            "uuid": "2e018334-6c7c-4382-bd45-a303e76e1d0b",
        },
    },
    "2022-2023": {
        "womens": {
            "name": "EH Adult EHL Womens",
            "uuid": "802809ef-f77f-45a3-a33c-1c8a2457316c",
        },
        "mens": {
            "name": "EH Adult EHL Open/Mens",
            "uuid": "8bbe49b0-0707-4cfb-b40d-9b52b2fd841f",
        },
    },
}

# Default path (works for all seasons - website uses query params)
DEFAULT_PATH: Final[str] = "2025-2026-4585604-eh-adult-ehl-womens-group-4585802-womens-premier-division"


# =============================================================================
# WOMEN'S COMPETITIONS (Season-Specific UUIDs)
# =============================================================================

WOMENS_COMPETITIONS: Final[Dict[str, Dict[str, Dict[str, str]]]] = {
    "2025-2026": {
        "premier": {
            "name": "Womens Premier Division",
            "uuid": "3a9366a6-1877-413e-b94e-816229092c51",
        },
        "premier_phase2_top6": {
            "name": "Womens Premier Division Phase 2 Top 6",
            "uuid": "a62d31b0-7e0b-4c23-a529-0e0a3498f63e",
        },
        "premier_phase2_lower6": {
            "name": "Womens Premier Division Phase 2 Lower 6",
            "uuid": "f9f65627-dd7c-4f3b-9da1-4d39f72c1f08",
        },
        "div1_north": {
            "name": "Womens Division 1 North",
            "uuid": "9fba25c1-c8e9-486c-be1a-a017e8d7e0e8",
        },
        "div1_south": {
            "name": "Womens Division 1 South",
            "uuid": "f9cacb04-850a-4b0c-9c86-22ca743091c3",
        },
    },
    "2024-2025": {
        "premier": {
            "name": "Womens Premier Division",
            "uuid": "5a357624-00ec-4010-a0d0-648294280748",
        },
        "premier_phase2_top6": {
            "name": "Womens Premier Division Phase 2 Top 6",
            "uuid": "8fd3cf25-a237-4520-bde3-3b4bd4c0c51e",
        },
        "premier_phase2_lower6": {
            "name": "Womens Premier Division Phase 2 Lower 6",
            "uuid": "a7a00558-769e-4a55-8aff-9f137330c00d",
        },
        "div1_north": {
            "name": "Womens Division 1 North",
            "uuid": "fab4b4af-d4c2-41cb-929a-f67e709c1521",
        },
        "div1_south": {
            "name": "Womens Division 1 South",
            "uuid": "037bef77-0d28-4f0d-ab6e-4c782dc4c683",
        },
    },
}


# =============================================================================
# MEN'S COMPETITIONS (Season-Specific UUIDs)
# =============================================================================

MENS_COMPETITIONS: Final[Dict[str, Dict[str, Dict[str, str]]]] = {
    "2025-2026": {
        "premier": {
            "name": "Open - Mens Premier Division",
            "uuid": "fd81a180-20a9-4323-b39d-252a048deaf0",
        },
        "premier_phase2_top6": {
            "name": "Open - Mens Premier Division Phase 2 Top 6",
            "uuid": "bb1cf609-554e-4d25-a056-bf7415b5407b",
        },
        "premier_phase2_lower6": {
            "name": "Open - Mens Premier Division Phase 2 Lower 6",
            "uuid": "4313d4b8-866e-4220-b182-f2bdce25b21c",
        },
        "div1_north": {
            "name": "Open - Mens Division 1 North",
            "uuid": "0aeadbb0-9e88-4fe3-a5fd-8afc5cf4cf5f",
        },
        "div1_south": {
            "name": "Open - Mens Division 1 South",
            "uuid": "d345f87e-1d6c-4cd5-9a0a-8bf6f962ff7b",
        },
    },
    "2024-2025": {
        "premier": {
            "name": "Open - Mens Premier Division",
            "uuid": "f15c4870-c47f-4ff8-9076-ae80ffdd959f",
        },
        "premier_phase2_top6": {
            "name": "Open - Mens Premier Division Phase 2 Top 6",
            "uuid": "985ecff8-5608-4852-8018-46af6e6ad486",
        },
        "premier_phase2_lower6": {
            "name": "Open - Mens Premier Division Phase 2 Lower 6",
            "uuid": "d2ca3b57-ab5c-43de-b3e2-e7b764d44946",
        },
        "div1_north": {
            "name": "Open - Mens Division 1 North",
            "uuid": "da58bbf6-e619-4765-8a1b-974b05f40a78",
        },
        "div1_south": {
            "name": "Open - Mens Division 1 South",
            "uuid": "8afe31ef-bb4f-42cc-94ff-a237d4b0514d",
        },
    },
}


# =============================================================================
# PRE-BUILT FILTER CONFIGS (Common Use Cases)
# =============================================================================

def build_filter_config(
    gender: str,
    competition_key: str,
    season: str = CURRENT_SEASON,
) -> FilterConfig:
    """
    Build a FilterConfig from gender, competition key, and season.

    Args:
        gender: "womens" or "mens"
        competition_key: Key like "premier", "div1_north", etc.
        season: Season string (default: current season)

    Returns:
        FilterConfig ready for use in scrapers

    Example:
        config = build_filter_config("womens", "premier")
        config = build_filter_config("mens", "div1_north", "2024-2025")
    """
    if gender not in ("womens", "mens"):
        raise ValueError(f"gender must be 'womens' or 'mens', got: {gender}")

    if season not in SEASONS:
        raise ValueError(f"Unknown season: {season}")

    # Get season-specific competition group
    if season not in COMP_GROUPS:
        raise ValueError(f"No competition groups defined for season: {season}")
    comp_group = COMP_GROUPS[season][gender]

    # Get season-specific competition
    competitions = WOMENS_COMPETITIONS if gender == "womens" else MENS_COMPETITIONS
    if season not in competitions:
        raise ValueError(f"No {gender} competitions defined for season: {season}")
    if competition_key not in competitions[season]:
        raise ValueError(f"Unknown competition: {competition_key} for season {season}")

    comp = competitions[season][competition_key]

    return FilterConfig(
        season_name=season,
        season_uuid=SEASONS[season],
        competition_group_name=comp_group["name"],
        competition_group_uuid=comp_group["uuid"],
        competition_name=comp["name"],
        competition_uuid=comp["uuid"],
        path=DEFAULT_PATH,  # Path works for all - website uses query params
    )


# Pre-built configs for Premier Divisions (most common use case)
WOMENS_PREMIER_FILTER = build_filter_config("womens", "premier")
MENS_PREMIER_FILTER = build_filter_config("mens", "premier")

# Default filter for testing
DEFAULT_FILTER = WOMENS_PREMIER_FILTER


# =============================================================================
# ALL COMPETITIONS CONFIG BUILDER
# =============================================================================

def build_all_competitions_config(
    gender: str,
    season: str = CURRENT_SEASON,
) -> AllCompetitionsConfig:
    """
    Build an AllCompetitionsConfig for scraping all matches in one page.

    Args:
        gender: "womens" or "mens"
        season: Season string (default: current season)

    Returns:
        AllCompetitionsConfig ready for use in matches scraper

    Example:
        config = build_all_competitions_config("womens", "2024-2025")
    """
    if gender not in ("womens", "mens"):
        raise ValueError(f"gender must be 'womens' or 'mens', got: {gender}")

    if season not in SEASONS:
        raise ValueError(f"Unknown season: {season}")

    if season not in COMP_GROUPS:
        raise ValueError(f"No competition groups defined for season: {season}")

    comp_group = COMP_GROUPS[season][gender]

    return AllCompetitionsConfig(
        season_name=season,
        season_uuid=SEASONS[season],
        competition_group_name=comp_group["name"],
        competition_group_uuid=comp_group["uuid"],
        path=DEFAULT_PATH,
    )


def get_all_competitions_configs(
    gender: str = "both",
    season: str = "current",
) -> list:
    """
    Get list of AllCompetitionsConfig for matches scraping.

    Args:
        gender: "womens", "mens", or "both"
        season: "current", "prior", "all" (all 4 seasons), or specific like "2024-2025"

    Returns:
        List of AllCompetitionsConfig objects
    """
    configs = []

    # Determine seasons
    if season == "current":
        seasons = ["2025-2026"]
    elif season == "prior":
        seasons = ["2024-2025"]
    elif season == "both":
        seasons = ["2025-2026", "2024-2025"]
    elif season == "all":
        seasons = ["2025-2026", "2024-2025", "2023-2024", "2022-2023"]
    else:
        seasons = [season]  # Specific season provided

    # Determine genders
    genders = []
    if gender in ("womens", "both"):
        genders.append("womens")
    if gender in ("mens", "both"):
        genders.append("mens")

    # Build configs
    for s in seasons:
        for g in genders:
            if s in COMP_GROUPS and g in COMP_GROUPS[s]:
                configs.append(build_all_competitions_config(g, s))

    return configs


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def get_all_competitions(gender: str = None, season: str = CURRENT_SEASON) -> Dict[str, FilterConfig]:
    """
    Get all competition configs for a season, optionally filtered by gender.

    Args:
        gender: Optional "womens" or "mens" to filter
        season: Season to get competitions for (default: current)

    Returns:
        Dict mapping "gender_competition" to FilterConfig
    """
    configs = {}

    if gender is None or gender == "womens":
        if season in WOMENS_COMPETITIONS:
            for key in WOMENS_COMPETITIONS[season]:
                configs[f"womens_{key}"] = build_filter_config("womens", key, season)

    if gender is None or gender == "mens":
        if season in MENS_COMPETITIONS:
            for key in MENS_COMPETITIONS[season]:
                configs[f"mens_{key}"] = build_filter_config("mens", key, season)

    return configs


def list_competitions() -> None:
    """Print all available competitions."""
    for season in ["2025-2026", "2024-2025"]:
        print(f"\n=== {season} ===")

        print("\nWomen's Competitions:")
        if season in WOMENS_COMPETITIONS:
            for key, comp in WOMENS_COMPETITIONS[season].items():
                print(f"  {key}: {comp['name']}")

        print("\nMen's Competitions:")
        if season in MENS_COMPETITIONS:
            for key, comp in MENS_COMPETITIONS[season].items():
                print(f"  {key}: {comp['name']}")


# =============================================================================
# EXPORTS
# =============================================================================

__all__ = [
    # Dataclasses
    "FilterConfig",
    "AllCompetitionsConfig",
    # Constants
    "SEASONS",
    "CURRENT_SEASON",
    "CURRENT_SEASON_UUID",
    "COMP_GROUPS",
    "WOMENS_COMPETITIONS",
    "MENS_COMPETITIONS",
    "DEFAULT_PATH",
    # Pre-built configs
    "WOMENS_PREMIER_FILTER",
    "MENS_PREMIER_FILTER",
    "DEFAULT_FILTER",
    # Functions
    "build_filter_config",
    "build_all_competitions_config",
    "get_all_competitions_configs",
    "get_all_competitions",
    "list_competitions",
]
