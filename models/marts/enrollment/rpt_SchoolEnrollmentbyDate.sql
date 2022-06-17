WITH 
  schools AS (
    SELECT
      SchoolId,
      SchoolName,
      SchoolNameMid,
      SchoolNameShort
    FROM {{ref('dim_Schools')}}
  ),

  enrollment AS (
    SELECT * FROM {{ref('fct_SchoolEnrollmentByDate')}}
  ),

  final AS (
    SELECT
      s.*,
      e.* EXCEPT(SchoolId)
    FROM schools AS s
    RIGHT JOIN enrollment AS e
    USING (SchoolId)
  )

SELECT * FROM final
