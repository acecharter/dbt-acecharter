WITH
  entities AS (
    SELECT * FROM {{ ref('dim_Entities')}}
  ),

  enrollment AS (
    SELECT * FROM {{ ref('int_CdeEnrByRaceAndGrade__1_unioned') }}
  ),

  final AS (
    SELECT
      enr.*
    FROM enrollment AS enr
    LEFT JOIN entities
    ON enr.EntityCode = entities.EntityCode
    WHERE entities.EntityCode IS NOT NULL
  )

SELECT * FROM final
