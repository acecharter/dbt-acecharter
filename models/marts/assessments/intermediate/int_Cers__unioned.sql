--NOTE: HS file for 2122 is currently excluded since file was blank as of last update

{{ config(
    materialized='table'
)}}

WITH

  cers_1819 AS(
    SELECT * FROM {{ ref('stg_RD__Cers1819') }}
  ),

  cers_1920 AS(
    SELECT * FROM {{ ref('stg_RD__Cers1920') }}
  ),

  cers_2021 AS(
    SELECT * FROM {{ ref('stg_RD__Cers2021') }}
  ),

  cers_2122 AS(
    SELECT * FROM {{ ref('stg_RD__Cers2122') }}
  ),

  final AS (
    SELECT * FROM cers_1819 
    UNION ALL
    SELECT * FROM cers_1920
    UNION ALL
    SELECT * FROM cers_2021
    UNION ALL
    SELECT * FROM cers_2122
       
  )

SELECT * FROM final
