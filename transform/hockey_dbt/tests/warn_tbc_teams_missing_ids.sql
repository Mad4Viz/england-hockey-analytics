-- Warns when TBC (To Be Confirmed) teams have null IDs
-- This is expected - source data quality issue from England Hockey website
-- TBC teams are placeholders for matches where participants depend on earlier results
{{ config(severity='warn') }}

SELECT
    f.match_id,
    s.home_team,
    s.away_team,
    f.match_date,
    'TBC teams from England Hockey source have no team data' AS warning_reason
FROM {{ ref('fct_matches') }} f
JOIN {{ ref('stg_matches') }} s ON f.match_id = s.match_id
WHERE (f.home_team_id IS NULL AND s.home_team LIKE 'TBC%')
   OR (f.away_team_id IS NULL AND s.away_team LIKE 'TBC%')
