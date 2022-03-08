WITH 
  schools AS (
    SELECT
      SchoolId,
      SchoolName,
      SchoolNameMid,
      SchoolNameShort
    FROM {{ ref('dim_Schools')}}
  ),

  caaspp AS (
    SELECT
      CAST(CAST(SchoolCode AS INT64) AS STRING) AS SchoolId,
      TestYear,
      DemographicId,
      StudentGroup,
      DemographicName,
      GradeLevel,
      TestId,
      TestSubject,
      StudentsEnrolled,
      StudentsWithScores,
      ReportingMethod,
      ResultDataType,
      SchoolResult,
      StudentWithResultCount
    FROM {{ ref('fct_SchoolCaaspp')}} 
  ),

  final AS (
    SELECT
      s.*,
      c.* EXCEPT (SchoolId)
    FROM schools AS s
    LEFT JOIN caaspp AS c
    USING (SchoolId)
  )

SELECT * FROM final
