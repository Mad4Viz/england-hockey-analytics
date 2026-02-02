{{ config(materialized='table') }}

{#
  dim_phases - Phase dimension table

  Purpose: Provides descriptive attributes and sort order for competition phases
  Grain: One row per unique phase

  PK: phase_id (surrogate key)

  Current Scope: Premier League phases only
    - Phase 1 = order 1 (initial round-robin)
    - Phase 2 = order 2 (second round-robin)

  Future Expansion (not yet implemented):
    - Top Six / Lower Six (league split phases)
    - Pool A / Pool B / Pool C (playoff pools)
    - Finals (championship)
    - Other divisions beyond Premier League

  Source: stg_standings (authoritative list of phases)
#}

WITH distinct_phases AS (
    SELECT DISTINCT phase AS phase_name
    FROM {{ ref('stg_standings') }}
    WHERE phase IS NOT null
),

phases_with_order AS (
    SELECT
        phase_name,

        -- Phase order for sorting
        -- Currently supports Premier League Phase 1 & 2
        CASE phase_name
            WHEN 'Phase 1' THEN 1
            WHEN 'Phase 2' THEN 2
            ELSE 99  -- Other phases: revisit when expanding scope
        END AS phase_order

    FROM distinct_phases
),

phases AS (
    SELECT
        -- PK: Numeric ID for joins
        phase_name,

        phase_order,
        ROW_NUMBER() OVER (ORDER BY phase_order, phase_name) AS phase_id

    FROM phases_with_order
)

SELECT * FROM phases
