WITH current_students AS (
    SELECT *
    FROM {{ ref('dim_Students') }}
    WHERE IsCurrentlyEnrolled = true
),

caaspp AS (
    SELECT * EXCEPT (AdministrationDate)
    FROM {{ ref('fct_StudentAssessment')}}
    WHERE
      AceAssessmentId IN ('1', '2')
),

levels AS (
    SELECT *
    FROM caaspp
    WHERE ReportingMethod = 'Achievement Level'
),

scale_scores AS (
    SELECT *
    FROM caaspp
    WHERE ReportingMethod = 'Scale Score'
),

dfs AS (
    SELECT *
    FROM caaspp
    WHERE ReportingMethod = 'Distance From Standard'
),

schools AS (
    SELECT
      SchoolId,
      SchoolName,
      SchoolNameMid,
      SchoolNameShort
    FROM {{ ref('dim_Schools')}}
)


SELECT
  s.* EXCEPT (SchoolId),
  cs.* EXCEPT (
      ExitWithdrawDate,
      ExitWithdrawReason
    ),
  l.* EXCEPT (StateUniqueId, TestedSchoolId, ReportingMethod, StudentResultDataType, StudentResult),
  l.StudentResult AS AchievementLevel,
  ss.StudentResult AS ScaleScore,
  dfs.StudentResult AS DistanceFromStandard
FROM current_students AS cs
LEFT JOIN levels AS l
ON cs.StateUniqueId = l.StateUniqueId
LEFT JOIN schools AS s
ON cs.SchoolId = s.SchoolId
LEFT JOIN scale_scores AS ss
ON l.AssessmentId = ss.AssessmentId
LEFT JOIN dfs
ON l.AssessmentId = dfs.AssessmentId
WHERE l.AssessmentId IS NOT NULL
