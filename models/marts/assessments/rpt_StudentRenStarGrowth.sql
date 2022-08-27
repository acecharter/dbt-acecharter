WITH
  schools AS (
    SELECT
      SchoolYear,
      SchoolId,
      SchoolName,
      SchoolNameMid,
      SchoolNameShort
    FROM {{ ref('dim_Schools')}}
  ),

  students AS (
    SELECT *
    FROM {{ ref('dim_Students') }}
  ),

  assessments AS (
    SELECT
      a.AceAssessmentId,
      a.AssessmentName,
      a.StateUniqueId,
      a.TestedSchoolId,
      s.SchoolNameMid AS TestedSchoolName,
      a.AssessmentSchoolYear,
      a.AssessmentId,
      a.AssessmentDate,
      a.GradeLevelWhenAssessed,
      a.AssessmentGradeLevel,
      a.AssessmentSubject,
      a.AssessmentObjective,
      a.ReportingMethod,
      CAST(a.StudentResult AS INT) AS StudentResult,
      CASE
        WHEN CAST(a.StudentResult AS INT) > 65 THEN 'High Growth'
        WHEN
          CAST(a.StudentResult AS INT) >= 50 AND
          CAST(a.StudentResult AS INT) <= 65
        THEN 'Above Average Growth'
        WHEN
          CAST(a.StudentResult AS INT) >= 35 AND
          CAST(a.StudentResult AS INT) < 50
        THEN 'Below Average Growth'
        WHEN CAST(a.StudentResult AS INT) < 35 THEN 'Low Growth'
      END AS GrowthLevel,
      CASE
        WHEN CAST(a.StudentResult AS INT) >= 35 THEN 'Yes'
        ELSE 'No' 
      END AS AtOrAboveAverageGrowth
    FROM {{ ref('fct_StudentAssessment')}} AS a
    LEFT JOIN schools AS s
    ON 
      a.TestedSchoolId = s.SchoolId
      AND a.AssessmentSchoolYear = s.SchoolYear
    WHERE
      a.ReportingMethod LIKE 'SGP%'
      AND a.ReportingMethod != 'SGP (current)'
  ),

  final AS (
    SELECT
      s.* EXCEPT (SchoolYear, SchoolId),
      st.* EXCEPT (SchoolYear),
      a.* EXCEPT (StateUniqueId)
    FROM students AS st
    LEFT JOIN assessments AS a
    ON st.StateUniqueId = a.StateUniqueId
    AND st.SchoolId = a.TestedSchoolId
    AND st.SchoolYear = a.AssessmentSchoolYear
    LEFT JOIN schools AS s
    ON st.SchoolId = s.SchoolId
    AND st.SchoolYear = s.SchoolYear
    WHERE a.AceAssessmentId IS NOT NULL
  )

SELECT * FROM final