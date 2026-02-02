{{ config(materialized='table') }}

WITH matches AS (
    SELECT * FROM {{ ref('int_matches') }}
),

-- home team perspective
home_teams AS (
    SELECT
        match_id,
        season,
        phase,
        competition_group,
        competition,
        match_date,
        match_time,
        venue,
        match_url,
        scraped_at,
        match_status,
        home_team AS team,
        away_team AS opponent,
        true AS is_home,
        'Home' AS home_away
    FROM matches
),

-- away team perspective
away_teams AS (
    SELECT
        match_id,
        season,
        phase,
        competition_group,
        competition,
        match_date,
        match_time,
        venue,
        match_url,
        scraped_at,
        match_status,
        away_team AS team,
        home_team AS opponent,
        false AS is_home,
        'Away' AS home_away
    FROM matches
),

combined AS (
    SELECT * FROM home_teams
    UNION ALL
    SELECT * FROM away_teams
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['match_id', 'team']) }} AS team_match_id,
    *
FROM combined
