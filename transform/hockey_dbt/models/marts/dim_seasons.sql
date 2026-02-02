{{ config(materialized='table') }}

{#
  dim_seasons - Season dimension table

  Purpose: Provides descriptive attributes for each hockey season
  Grain: One row per season

  PK: season_id (surrogate key)

  Source: stg_standings (authoritative list of seasons)
          stg_matches (for date range derivation)
#}

WITH seasons_from_standings AS (
    -- Authoritative list of seasons from standings
    SELECT DISTINCT season
    FROM {{ ref('stg_standings') }}
    WHERE season IS NOT null
),

season_dates AS (
    -- Get actual date range for each season from match data
    SELECT
        season,
        MIN(match_date) AS start_date,
        MAX(match_date) AS end_date
    FROM {{ ref('stg_matches') }}
    GROUP BY season
),

seasons AS (
    SELECT
        -- PK: Numeric ID for joins (ordered chronologically)
        s.season AS season_name,

        -- Descriptive attributes
        d.start_date,
        d.end_date,
        ROW_NUMBER() OVER (ORDER BY s.season) AS season_id

    FROM seasons_from_standings AS s
    LEFT JOIN season_dates AS d ON s.season = d.season
)

SELECT * FROM seasons
