{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM {{ source('hockey_raw', 'matches') }}
),

staged AS (
    SELECT
        -- surrogate key (unique identifier for each match)
        {{ dbt_utils.generate_surrogate_key(['season', 'match_date', 'home_team', 'away_team']) }} AS match_id,

        -- dimensions (text fields that describe the data)
        CAST(season AS string) AS season,
        CAST(phase AS string) AS phase,
        CAST(competition_group AS string) AS competition_group,
        CAST(competition AS string) AS competition,
        CAST(home_team AS string) AS home_team,
        CAST(away_team AS string) AS away_team,
        CAST(venue AS string) AS venue,

        -- date/time fields
        CAST(match_date AS date) AS match_date,
        CAST(match_time AS string) AS match_time,

        -- measures (numeric fields)
        CAST(home_score AS int64) AS home_score,
        CAST(away_score AS int64) AS away_score,

        -- metadata
        match_url,
        CAST(scraped_at AS timestamp) AS scraped_at

    FROM source
)

SELECT * FROM staged
