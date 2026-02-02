{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM {{ source('hockey_raw', 'match_events') }}
),

staged AS (
    SELECT
        -- surrogate key (unique identifier for each event)
        {{ dbt_utils.generate_surrogate_key(['match_url', 'event_type', 'team', 'player_name', 'minute']) }} AS event_id,

        -- dimensions (text fields that describe the data)
        CAST(season AS string) AS season,
        CAST(event_type AS string) AS event_type,
        CAST(event_subtype AS string) AS event_subtype,
        CAST(team AS string) AS team,
        CAST(player_name AS string) AS player_name,
        CAST(home_team AS string) AS home_team,
        CAST(away_team AS string) AS away_team,

        -- measures (numeric fields)
        CAST(minute AS int64) AS minute,

        -- display fields (for Tableau image rendering)
        home_logo,
        away_logo,

        -- metadata
        match_url,
        CAST(match_date AS date) AS match_date,
        CAST(scraped_at AS timestamp) AS scraped_at

    FROM source
)

SELECT * FROM staged
