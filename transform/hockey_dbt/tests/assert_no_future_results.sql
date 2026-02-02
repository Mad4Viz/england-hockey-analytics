-- Custom test: Matches in the future should not have scores
-- A passing test returns 0 rows
-- If this test fails, it means we have data integrity issues:
-- either the match_date is wrong, or scores were entered prematurely

select
    match_id,
    match_date,
    home_score,
    away_score,
    match_status
from {{ ref('fct_matches') }}
where match_date > current_date()
  and (home_score is not null or away_score is not null)
