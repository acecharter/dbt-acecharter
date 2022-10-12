WITH 
  anet_m AS (
    SELECT *
    FROM {{ ref('stg_RD__Anet')}} 
    WHERE scored_by = 'Machine'
  ),

  final AS (
    SELECT
      AceAssessmentId,
      AceAssessmentName,
      school_year AS Year,
      CONCAT(CAST(school_year AS STRING), "-", CAST(school_year - 1999 AS STRING)) AS SchoolYear,
      StateSchoolCode,
      school_name AS SchoolName,
      CAST(sas_id AS STRING) AS StateUniqueId,
      CAST(sis_id AS STRING) AS StudentUniqueId,
      student_first_name AS FirstName,
      student_middle_name AS MiddleName,
      student_last_name AS LastName,
      CAST(enrollment_grade AS INT64) AS GradeLevel,
      CASE
        WHEN course = 'english_i' THEN 'English I'
        WHEN course = 'english_ii' THEN 'English II'
        WHEN course = 'english_iii' THEN 'English III'
        WHEN course = 'algebra_i' THEN 'Algebra I'
        WHEN course = 'geometry' THEN 'Geometry'
        ELSE course
      END AS Course,
      period AS Period,
      teacher_first_name AS TeacherFirstName,
      teacher_last_name AS TeacherLastName,
      cycle AS Cycle,
      subject AS Subject,
      assessment_id AS AssessmentId,
      assessment_name AS AssessmentName,
      SUM(points_received) AS PointsReceived,
      SUM(points_possible) AS PointsPossible,
      ROUND(SUM(points_received)/SUM(points_possible), 2) AS Score
    FROM anet_m
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20
  )

SELECT *
FROM final
ORDER BY cycle