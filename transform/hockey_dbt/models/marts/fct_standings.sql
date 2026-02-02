{{ config(materialized='table') }}

{#
  fct_standings - Standings fact table (CHILD FACT)

  Purpose: League table positions and statistics
  Grain: One row per team per season per phase

  PK: standings_id
  FKs: season_id, phase_id, team_id (no fact-to-fact relationship)

  Note: This fact stands alone - it doesn't reference fct_matches.
  It represents aggregated standings data scraped from the league website.

  Source: stg_standings
  Joins: dim_seasons, dim_phases, dim_teams
#}

WITH standings AS (
    SELECT * FROM {{ ref('stg_standings') }}
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

-- Join to dimensions for surrogate keys
final AS (
    SELECT
        -- PK (rename for consistency with DBML)
        st.standing_id AS standings_id,

        -- FKs
        s.season_id,
        p.phase_id,
        t.team_id,

        -- Measures
        st.position,
        st.played,
        st.won,
        st.drawn,
        st.lost,
        st.goals_for,
        st.goals_against,
        st.goal_difference,
        st.points,

        -- Metadata
        DATE(st.scraped_at) AS snapshot_date

    FROM standings AS st

    -- Dimension joins
    LEFT JOIN seasons AS s ON st.season = s.season_name
    LEFT JOIN phases AS p ON st.phase = p.phase_name
    LEFT JOIN teams AS t ON st.team = t.team_name
)

SELECT * FROM final
