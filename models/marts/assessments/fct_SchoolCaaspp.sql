WITH 
  caaspp AS (
    SELECT * FROM {{ ref('int_Caaspp__unioned_filtered_melted')}} 
  ),

  entities AS (
    SELECT *
    FROM {{ ref('stg_RD__CaasppEntities')}}
    ),

  demographics AS (
    SELECT *
    FROM {{ ref('stg_RD__CaasppStudentGroups')}}
  ),

  final AS (
    SELECT
      e.TypeId,
      e.CountyCode,
      e.DistrictCode,
      e.SchoolCode,
      e.CountyName,
      e.DistrictName,
      e.SchoolName,
      e.TestYear,
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
      c.Result,
      c.StudentWithResultCount
    FROM caaspp AS c
    LEFT JOIN entities AS e
    ON
      c.CountyCode = e.CountyCode AND
      c.DistrictCode = e.DistrictCode AND
      c.SchoolCode = e.SchoolCode AND
      c.TestYear = e.TestYear
    LEFT JOIN demographics AS d
    ON c.DemographicId = d.DemographicId
  )

SELECT * FROM final