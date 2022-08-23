{{ config(
    materialized='table'
)}}

WITH reading_unioned AS(
  SELECT * FROM {{ ref('stg_RD__RenStarReading2021')}}
  UNION ALL
  SELECT * FROM {{ ref('stg_RSA__Reading_v2_SY22')}}
  UNION ALL
  SELECT * FROM {{ ref('stg_RSA__ReadingSpanish_v2_SY22')}}
  UNION ALL
  SELECT * FROM {{ ref('stg_RS__Reading_v2')}}
  UNION ALL
  SELECT * FROM {{ ref('stg_RS__ReadingSpanish_v2')}}
),

math_unioned AS(
  SELECT * FROM {{ ref('stg_RD__RenStarMath2021')}}
  UNION ALL
  SELECT * FROM {{ ref('stg_RSA__Math_v2_SY22')}}
  UNION ALL
  SELECT * FROM {{ ref('stg_RSA__MathSpanish_v2_SY22')}}
  UNION ALL
  SELECT * FROM {{ ref('stg_RS__Math_v2')}}
  UNION ALL
  SELECT * FROM {{ ref('stg_RS__MathSpanish_v2')}}
),

reading AS (
  SELECT
    * EXCEPT(
      InstructionalReadingLevel, 
      Lexile
    ),
    InstructionalReadingLevel,
    Lexile,
    CAST(NULL AS STRING) AS Quantile,
  FROM reading_unioned
),

math AS (
  SELECT
    * EXCEPT(Quantile),
    CAST(NULL AS STRING) AS InstructionalReadingLevel,
    CAST(NULL AS STRING) AS Lexile,
    Quantile,
  FROM math_unioned
),

final AS (
  SELECT * FROM reading
  UNION ALL
  SELECT * FROM math
)

SELECT * FROM final