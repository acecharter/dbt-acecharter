WITH 
  elpac AS (
    SELECT * FROM {{ ref('int_Elpac__3_melted')}} 
  ),

  demographics AS (
    SELECT *
    FROM {{ ref('stg_RD__ElpacStudentGroups')}}
  ),

  final AS (
    SELECT
      e.EntityCode,
      e.EntityType,
      e.EntityName,
      e.EntityNameMid,
      e.EntityNameShort,
      e.RecordType,
      e.TestYear,
      e.SchoolYear,
      e.StudentGroupId,
      d.StudentGroupName AS StudentGroup,
      e.GradeLevel,
      e.AssessmentType,
      e.TotalEnrolled,
      e.TotalTestedWithScores,
      e.ReportingMethod,
      e.ResultDataType,
      e.SchoolResult,
      e.StudentWithResultCount
    FROM elpac AS e
    LEFT JOIN demographics AS d
    ON e.StudentGroupId = d.StudentGroupId
  )

SELECT * FROM final