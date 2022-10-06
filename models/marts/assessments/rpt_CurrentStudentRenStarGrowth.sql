WITH
  schools AS (
    SELECT DISTINCT
      SchoolId,
      SchoolName,
      SchoolNameMid,
      SchoolNameShort
    FROM {{ ref('dim_Schools')}}
  ),

  students AS (
    SELECT *
    FROM {{ ref('dim_Students')}}
    WHERE IsCurrentlyEnrolled = TRUE
  ),

  assessments AS (
    SELECT
      AceAssessmentId,
      AssessmentName,
      StateUniqueId,
      TestedSchoolId,
      AssessmentSchoolYear,
      AssessmentId,
      AssessmentDate,
      GradeLevelWhenAssessed,
      AssessmentGradeLevel,
      AssessmentSubject,
      AssessmentObjective,
      ReportingMethod,
      CAST(StudentResult AS INT) AS StudentResult,
      CASE
        WHEN CAST(StudentResult AS INT) > 65 THEN 'High Growth'
        WHEN
          CAST(StudentResult AS INT) >= 50 AND
          CAST(StudentResult AS INT) <= 65
        THEN 'Above Average Growth'
        WHEN
          CAST(StudentResult AS INT) >= 35 AND
          CAST(StudentResult AS INT) < 50
        THEN 'Below Average Growth'
        WHEN CAST(StudentResult AS INT) < 35 THEN 'Low Growth'
      END AS GrowthLevel,
      CASE
        WHEN CAST(StudentResult AS INT) >= 35 THEN 'Yes'
        ELSE 'No' 
      END AS AtOrAboveAverageGrowth
    FROM {{ ref('fct_StudentAssessment')}}
    WHERE
      ReportingMethod LIKE 'SGP%'
      AND ReportingMethod != 'SGP (current)'
  ),

  final AS (
    SELECT
      s.* EXCEPT (SchoolId),
      st.* EXCEPT (SchoolYear),
      a.AceAssessmentId,
      a.AssessmentName,
      a.TestedSchoolId,
      CASE WHEN ts.SchoolId IS NOT NULL THEN ts.SchoolNameMid ELSE 'Non-ACE School' END AS TestedSchoolName,
      a.* EXCEPT (AceAssessmentId, AssessmentName, TestedSchoolId, StateUniqueId)
    FROM students AS st
    LEFT JOIN assessments AS a
    ON st.StateUniqueId = a.StateUniqueId
    LEFT JOIN schools AS s
    ON st.SchoolId = s.SchoolId
    LEFT JOIN schools AS ts
    ON a.TestedSchoolId = ts.SchoolId
    WHERE a.AceAssessmentId IS NOT NULL
  )

SELECT * FROM final