{{ config(materialized='table') }}

WITH matches AS (
    SELECT * FROM {{ ref('stg_matches') }}
),

with_status AS (
    SELECT
        -- pass through all staging columns
        match_id,
        season,
        phase,
        competition_group,
        competition,
        home_team,
        away_team,
        venue,
        match_date,
        match_time,
        home_score,
        away_score,
        match_url,
        scraped_at,

        -- business logic: determine match status
        -- uses scraped_at to avoid dependency on when the model runs
        CASE
            WHEN home_score IS NOT null THEN 'Completed'
            WHEN match_date > DATE(scraped_at) THEN 'Upcoming'
            ELSE 'Missing result'
        END AS match_status

    FROM matches
)

SELECT * FROM with_status
