WITH 
  caaspp AS (
    SELECT * FROM {{ ref('int_Caaspp__3_melted')}} 
  ),

  demographics AS (
    SELECT *
    FROM {{ ref('stg_RD__CaasppStudentGroups')}}
  ),

  final AS (
    SELECT
      c.EntityCode,
      c.EntityType,
      c.EntityName,
      c.EntityNameMid,
      c.EntityNameShort,
      c.TypeId,
      c.TestYear,
      c.SchoolYear,
      c.DemographicId,
      d.StudentGroup,
      d.DemographicName,
      c.GradeLevel,
      c.TestId,
      CASE
        WHEN c.TestId = '1' THEN 'ELA'
        WHEN c.TestId = '2' Then 'Math'
      END AS TestSubject,
      c.StudentsEnrolled,
      c.StudentsWithScores,
      c.ReportingMethod,
      c.ResultDataType,
      c.SchoolResult,
      c.StudentWithResultCount
    FROM caaspp AS c
    LEFT JOIN demographics AS d
    ON c.DemographicId = d.DemographicId
  )

SELECT * FROM final