WITH reading_unioned AS(
  SELECT * FROM {{ ref('base_RSA__Reading_SY21')}}
  UNION ALL
  SELECT * FROM {{ ref('base_RSA__Reading_v2_SY22')}}
  UNION ALL
  SELECT * FROM {{ ref('base_RSA__ReadingSpanish_v2_SY22')}}
  UNION ALL
  SELECT * FROM {{ ref('base_RS__Reading_v2')}}
  UNION ALL
  SELECT * FROM {{ ref('base_RS__ReadingSpanish_v2')}}
),

math_unioned AS(
  SELECT * FROM {{ ref('base_RSA__Math_SY21')}}
  UNION ALL
  SELECT * FROM {{ ref('base_RSA__Math_v2_SY22')}}
  UNION ALL
  SELECT * FROM {{ ref('base_RSA__MathSpanish_v2_SY22')}}
  UNION ALL
  SELECT * FROM {{ ref('base_RS__Math_v2')}}
  UNION ALL
  SELECT * FROM {{ ref('base_RS__MathSpanish_v2')}}
),

early_literacy_unioned AS(
  SELECT * FROM {{ ref('base_RSA__EarlyLiteracy_SY21')}}
  UNION ALL
  SELECT * FROM {{ ref('base_RSA__EarlyLiteracy_SY22')}}
  UNION ALL
  SELECT * FROM {{ ref('base_RS__EarlyLiteracy_v2')}}
  UNION ALL
  SELECT * FROM {{ ref('base_RSA__EarlyLiteracySpanish_v2_SY22')}}
  UNION ALL
  SELECT * FROM {{ ref('base_RS__EarlyLiteracySpanish_v2')}}
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
    CAST(NULL AS STRING) AS LiteracyClassification
  FROM reading_unioned
),

math AS (
  SELECT
    * EXCEPT(Quantile),
    CAST(NULL AS STRING) AS InstructionalReadingLevel,
    CAST(NULL AS STRING) AS Lexile,
    Quantile,
    CAST(NULL AS STRING) AS LiteracyClassification
  FROM math_unioned
),

early_literacy AS (
  SELECT
    * EXCEPT(Lexile, LiteracyClassification),
    CAST(NULL AS STRING) AS InstructionalReadingLevel,
    Lexile,
    CAST(NULL AS STRING) AS Quantile,
    LiteracyClassification
  FROM early_literacy_unioned
),

final AS (
  SELECT * FROM reading
  UNION ALL
  SELECT * FROM math
  UNION ALL
  SELECT * FROM early_literacy
)

SELECT * FROM final