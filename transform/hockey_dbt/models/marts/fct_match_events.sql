{{ config(materialized='table') }}

{#
  fct_match_events - Match events fact table (CHILD FACT)

  Purpose: Individual events (goals, cards) within matches
  Grain: One row per event

  PK: event_id
  FKs: match_id→fct_matches, player_id→dim_players, team_id→dim_teams

  Source: stg_match_events
  Joins: stg_matches (for match_id), dim_players, dim_teams
#}

WITH events AS (
    SELECT * FROM {{ ref('stg_match_events') }}
),

-- Get match_id from stg_matches (join on match_url)
matches AS (
    SELECT
        match_id,
        match_url
    FROM {{ ref('stg_matches') }}
),

-- Dimension tables for FK lookups
players AS (
    SELECT * FROM {{ ref('dim_players') }}
),

teams AS (
    SELECT * FROM {{ ref('dim_teams') }}
),

-- Join to get match_id and dimension FKs
final AS (
    SELECT
        -- PK
        e.event_id,

        -- FKs
        m.match_id,  -- FK to fct_matches
        p.player_id,
        t.team_id,

        -- Attributes
        e.event_type,
        e.event_subtype,
        e.minute

    FROM events AS e

    -- Get match_id via match_url
    LEFT JOIN matches AS m ON e.match_url = m.match_url

    -- Dimension joins
    LEFT JOIN players AS p ON e.player_name = p.player_name
    LEFT JOIN teams AS t ON e.team = t.team_name
)

SELECT * FROM final
