{{ config(materialized='table') }}

{#
  dim_teams - Team dimension table

  Purpose: Master list of all teams across all seasons and divisions
  Grain: One row per unique team name

  PK: team_id (surrogate key)

  Note: Division is NOT stored here since teams can be promoted/relegated
  between seasons. Division context comes from match/standings tables.

  Source: stg_standings (authoritative list of teams in competitions)
#}

WITH distinct_teams AS (
    SELECT DISTINCT team AS team_name
    FROM {{ ref('stg_standings') }}
    WHERE team IS NOT null
),

teams AS (
    SELECT
        -- PK: Numeric ID for joins (ordered alphabetically)
        team_name,

        ROW_NUMBER() OVER (ORDER BY team_name) AS team_id

    FROM distinct_teams
)

SELECT * FROM teams
