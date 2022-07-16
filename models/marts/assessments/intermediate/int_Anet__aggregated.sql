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
      sas_id AS StateUniqueId,
      sis_id AS StudentUniqueId,
      student_first_name AS FirstName,
      student_middle_name AS MiddleName,
      student_last_name AS LastName,
      enrollment_grade AS GradeLevel,
      course AS Course,
      period AS Period,
      teacher_first_name AS TeacherFirstName,
      teacher_last_name AS TeacherLastName,
      cycle AS Cycle,
      subject AS Subject,
      assessment_id AS AssessmentId,
      assessment_name AS AssessmentName,
      SUM(points_received) AS PointsReceived,
      SUM(points_possible) AS PointsPossible,
      ROUND(SUM(points_received)/SUM(points_possible), 3) AS Score
    FROM anet_m
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18
  )

SELECT *
FROM final
ORDER BY cycle