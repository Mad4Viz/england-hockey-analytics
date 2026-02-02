{{ config(materialized='table') }}

{#
  dim_players - Player dimension table

  Purpose: Master list of all players who have scored or received cards
  Grain: One row per unique player name

  PK: player_id (surrogate key)

  Note: Players are derived from match events (goals, cards).
  A player only appears here if they've had an event recorded.

  Source: stg_match_events (authoritative list of players from events)
#}

WITH distinct_players AS (
    SELECT DISTINCT player_name
    FROM {{ ref('stg_match_events') }}
    WHERE player_name IS NOT null
),

players AS (
    SELECT
        -- PK: Numeric ID for joins (ordered alphabetically)
        player_name,

        ROW_NUMBER() OVER (ORDER BY player_name) AS player_id

    FROM distinct_players
)

SELECT * FROM players
