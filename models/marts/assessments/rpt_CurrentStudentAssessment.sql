WITH
  schools AS (
    SELECT DISTINCT
      SchoolId,
      SchoolName,
      SchoolNameMid,
      SchoolNameShort
    FROM {{ ref('dim_Schools')}}
  ),

  current_students AS (
    SELECT *
    FROM {{ ref('dim_Students') }}
    WHERE IsCurrentlyEnrolled = TRUE
  ),

  assessments AS (
    SELECT
      a.AceAssessmentId,
      a.AssessmentName,
      a.StateUniqueId,
      a.TestedSchoolId,
      CASE
        WHEN a.TestedSchoolId = s.SchoolId THEN s.SchoolNameShort
        ELSE 'Non-ACE School'
      END AS TestedSchoolName,
      a.AssessmentSchoolYear,
      a.AssessmentId,
      a.AssessmentDate,
      a.GradeLevelWhenAssessed,
      a.AssessmentGradeLevel,
      a.AssessmentSubject,
      a.AssessmentObjective,
      a.ReportingMethod,
      a.StudentResultDataType,
      a.StudentResult
    FROM {{ ref('fct_StudentAssessment')}} AS a
    LEFT JOIN schools AS s
    ON a.TestedSchoolId = s.SchoolId
  ),

  final AS (
    SELECT
      st.* EXCEPT (SchoolYear),
      a.* EXCEPT (StateUniqueId)
    FROM current_students AS st
    INNER JOIN assessments AS a
    ON st.StateUniqueId = a.StateUniqueId
  )

SELECT * FROM final
