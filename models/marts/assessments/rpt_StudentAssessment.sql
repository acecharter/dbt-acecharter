WITH
  current_schools AS (
    SELECT
      SchoolId,
      SchoolName,
      SchoolNameMid,
      SchoolNameShort
    FROM {{ ref('dim_CurrentSchools')}}
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
    LEFT JOIN current_schools AS s
    ON a.TestedSchoolId = s.SchoolId
  )


SELECT
  s.* EXCEPT (SchoolId),
  st.* EXCEPT (SchoolYear),
  a.* EXCEPT (StateUniqueId)
FROM current_students AS st
INNER JOIN assessments AS a
ON st.StateUniqueId = a.StateUniqueId
INNER JOIN current_schools AS s
ON st.SchoolId = s.SchoolId
