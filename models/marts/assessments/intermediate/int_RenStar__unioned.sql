WITH reading_unioned AS(
  SELECT * FROM {{ ref('stg_RawData__RenStarReading2021')}}
  UNION ALL
  SELECT * FROM {{ ref('stg_RenaissanceStar__Reading_v2')}}
),

math_unioned AS(
  SELECT * FROM {{ ref('stg_RawData__RenStarMath2021')}}
  UNION ALL
  SELECT * FROM {{ ref('stg_RenaissanceStar__Math_v2')}}
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
)

SELECT * FROM reading
UNION ALL
SELECT * FROM math