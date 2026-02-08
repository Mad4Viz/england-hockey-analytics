{{ config(materialized='table') }}

{#
  fct_matches - Match fact table (ANCHOR FACT)

  Purpose: Central fact table for match-level analysis
  Grain: One row per match

  PK: match_id (surrogate key from staging)
  FKs: season_id, phase_id, home_team_id, away_team_id

  This is the "anchor" fact - other facts (fct_team_matches, fct_match_events)
  will reference this table via match_id.

  Source: int_matches (has match_status business logic)
  Joins: dim_seasons, dim_phases, dim_teams (twice for home/away)
#}

WITH matches AS (
    SELECT * FROM {{ ref('int_matches') }}
),

-- Dimension tables for lookups
seasons AS (
    SELECT * FROM {{ ref('dim_seasons') }}
),

phases AS (
    SELECT * FROM {{ ref('dim_phases') }}
),

teams AS (
    SELECT * FROM {{ ref('dim_teams') }}
),

-- Join matches to dimensions to get surrogate keys
-- We join on NATURAL KEY (text) and select SURROGATE KEY (integer)
final AS (
    SELECT
        -- PK: Keep the match_id from staging/intermediate
        m.match_id,

        -- FKs: Replace natural keys with surrogate keys from dimensions
        -- This is the core pattern of fact tables in star schemas
        s.season_id,
        p.phase_id,
        home.team_id AS home_team_id,
        away.team_id AS away_team_id,

        -- Date/time attributes (kept on fact for filtering)
        m.match_date,
        m.match_time,

        -- Measures (the numbers we want to analyze)
        m.home_score,
        m.away_score,

        -- Competition context (denormalised for filtering)
        m.competition_group,
        m.competition AS competition_name,

        -- Descriptive attributes
        m.venue,
        m.match_status  -- Business logic added in intermediate layer

    FROM matches AS m

    -- Join to dim_seasons: match season text → season_id
    LEFT JOIN seasons AS s
        ON m.season = s.season_name

    -- Join to dim_phases: match phase text → phase_id
    LEFT JOIN phases AS p
        ON m.phase = p.phase_name

    -- Join to dim_teams TWICE: once for home team, once for away team
    -- This is called a "role-playing dimension" - same dimension, different roles
    LEFT JOIN teams AS home
        ON m.home_team = home.team_name

    LEFT JOIN teams AS away
        ON m.away_team = away.team_name
)

SELECT * FROM final
