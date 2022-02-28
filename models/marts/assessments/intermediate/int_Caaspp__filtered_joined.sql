WITH 
  caaspp_filtered AS (
    SELECT * FROM {{ ref('int_Caaspp__filtered')}} 
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
      ROUND(c.StudentsWithScores / c.StudentsEnrolled, 4) AS PctStudentsWithScores,
      c.MeanScaleScore,
      ROUND(c.PctStandardMetAndAbove * c.StudentsWithScores, 0) AS StudentsStandardMetAndAbove,
      c.PctStandardMetAndAbove
    FROM caaspp_filtered AS c
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