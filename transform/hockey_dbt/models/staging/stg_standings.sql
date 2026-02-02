{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM {{ source('hockey_raw', 'standings') }}
),

staged AS (
    SELECT
        -- surrogate key (unique identifier for each row)
        {{ dbt_utils.generate_surrogate_key(['season', 'phase', 'competition_group', 'competition', 'team']) }} AS standing_id,

        -- dimensions (text fields that describe the data)
        CAST(season AS string) AS season,
        CAST(phase AS string) AS phase,
        CAST(competition_group AS string) AS competition_group,
        CAST(competition AS string) AS competition,
        CAST(team AS string) AS team,

        -- measures (numeric fields)
        CAST(position AS int64) AS position,
        CAST(played AS int64) AS played,
        CAST(won AS int64) AS won,
        CAST(drawn AS int64) AS drawn,
        CAST(lost AS int64) AS lost,
        CAST(goals_for AS int64) AS goals_for,
        CAST(goals_against AS int64) AS goals_against,
        CAST(goal_difference AS int64) AS goal_difference,
        CAST(points AS int64) AS points,

        -- metadata
        standings_url,
        CAST(scraped_at AS timestamp) AS scraped_at

    FROM source
)

SELECT * FROM staged
