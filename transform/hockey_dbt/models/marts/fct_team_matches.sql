{{ config(materialized='table') }}

{#
  fct_team_matches - Team-perspective fact table (CHILD FACT)

  Purpose: One row per team per match - enables "team journey" analysis
  Grain: One row per team per match (200 rows = 100 matches × 2 teams)

  PK: team_match_id
  FKs: match_id→fct_matches, season_id, phase_id, team_id, opponent_id

  Note: This is a "bridge" table for team perspective.
  Season aggregates (total goals, points, etc.) live on fct_standings.

  Source: int_team_matches + int_matches (for scores)
  Joins: dim_seasons, dim_phases, dim_teams (twice for team/opponent)
#}

WITH team_matches AS (
    SELECT * FROM {{ ref('int_team_matches') }}
),

-- Get scores from int_matches
matches AS (
    SELECT
        match_id,
        home_score,
        away_score
    FROM {{ ref('int_matches') }}
),

-- Dimension tables for FK lookups
seasons AS (
    SELECT * FROM {{ ref('dim_seasons') }}
),

phases AS (
    SELECT * FROM {{ ref('dim_phases') }}
),

teams AS (
    SELECT * FROM {{ ref('dim_teams') }}
),

-- Join scores
with_scores AS (
    SELECT
        tm.*,
        m.home_score,
        m.away_score
    FROM team_matches AS tm
    LEFT JOIN matches AS m ON tm.match_id = m.match_id
),

-- Final: Join to dimensions for surrogate keys
final AS (
    SELECT
        -- PK
        tm.team_match_id,

        -- FKs: Replace natural keys with surrogate keys
        tm.match_id,  -- FK to fct_matches
        s.season_id,
        p.phase_id,
        t.team_id,
        opp.team_id AS opponent_id,

        -- Attributes
        tm.is_home,
        tm.match_date,
        tm.venue,

        -- Scores (denormalized for convenience)
        tm.home_score,
        tm.away_score

    FROM with_scores AS tm

    -- Dimension joins
    LEFT JOIN seasons AS s ON tm.season = s.season_name
    LEFT JOIN phases AS p ON tm.phase = p.phase_name
    LEFT JOIN teams AS t ON tm.team = t.team_name
    LEFT JOIN teams AS opp ON tm.opponent = opp.team_name
)

SELECT * FROM final
