WITH
  schools AS (
    SELECT
      SchoolId,
      SchoolName,
      SchoolNameMid,
      SchoolNameShort
    FROM {{ ref('dim_Schools')}}
  ),

  current_students AS (
    SELECT *
    FROM {{ ref('dim_Students') }}
    WHERE IsCurrentlyEnrolled = true
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
      a.AssessedGradeLevel,
      a.AssessmentSubject,
      a.AssessmentObjective,
      a.ReportingMethod,
      a.StudentResultDataType,
      a.StudentResult
    FROM {{ ref('fct_StudentAssessment')}} AS a
    LEFT JOIN schools AS s
    ON a.TestedSchoolId = s.SchoolId
  )


SELECT
  s.* EXCEPT (SchoolId),
  cs.* EXCEPT (
      ExitWithdrawDate,
      ExitWithdrawReason
    ),
  a.* EXCEPT (StateUniqueId)
FROM current_students AS cs
LEFT JOIN assessments AS a
ON cs.StateUniqueId = a.StateUniqueId
LEFT JOIN schools AS s
ON cs.SchoolId = s.SchoolId
