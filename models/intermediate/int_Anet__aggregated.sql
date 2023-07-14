WITH 
  anet_m AS (
    SELECT *
    FROM {{ ref('stg_RD__Anet')}} 
    WHERE ScoredBy = 'Machine'
  ),

  final AS (
    SELECT
      AceAssessmentId,
      AceAssessmentName,
      SchoolYear AS Year,
      CONCAT(CAST(SchoolYear AS STRING), "-", CAST(SchoolYear - 1999 AS STRING)) AS SchoolYear,
      StateSchoolCode,
      SchoolName,
      CAST(SasId AS STRING) AS StateUniqueId,
      CAST(SisId AS STRING) AS StudentUniqueId,
      StudentFirstName AS FirstName,
      StudentMiddleName AS MiddleName,
      StudentLastName AS LastName,
      CAST(EnrollmentGrade AS INT64) AS GradeLevel,
      CASE
        WHEN Course = 'english_i' THEN 'English I'
        WHEN Course = 'english_ii' THEN 'English II'
        WHEN Course = 'english_iii' THEN 'English III'
        WHEN Course = 'algebra_i' THEN 'Algebra I'
        WHEN Course = 'geometry' THEN 'Geometry'
        ELSE Course
      END AS Course,
      Period,
      TeacherFirstName,
      TeacherLastName,
      Cycle,
      Subject,
      AssessmentId,
      AssessmentName,
      SUM(PointsReceived) AS PointsReceived,
      SUM(PointsPossible) AS PointsPossible,
      ROUND(SUM(PointsReceived)/SUM(PointsPossible), 2) AS Score
    FROM anet_m
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20
  )

SELECT *
FROM final
ORDER BY cycle