WITH students AS (
  SELECT * FROM {{ ref('dim_Students') }}
),

schools AS (
    SELECT
      SchoolId,
      SchoolName,
      SchoolNameMid,
      SchoolNameShort
    FROM {{ ref('dim_Schools')}}
),

assessments AS (
  SELECT *
  FROM {{ ref('stg_GSD__Assessments')}}
),

testing_window_eligible_students AS (
  SELECT * FROM {{ ref('dim_RenStarTestingWindowEligibleStudents')}}
),

student_result_counts AS (
  SELECT * FROM {{ ref('fct_StudentRenStarResultCountsByTestingWindow')}}
)


SELECT
  sc.*,
  st.* EXCEPT (SchoolId),
  es.TestingWindowType,
  es.TestingWindowName,
  es.AceAssessmentId,
  a.AssessmentNameShort,
  a.AssessmentSubject,
  CASE WHEN rc.AssessmentResultCount IS NULL Then 'Not Tested' ELSE 'Tested' END AS WindowTestingStatus
FROM testing_window_eligible_students AS es
LEFT JOIN student_result_counts AS rc
ON
  es.StudentUniqueId = rc.StudentUniqueId AND
  es.SchoolId = rc.TestedSchoolId AND
  es.TestingWindowType = rc.TestingWindowType AND
  es.TestingWindowName = rc.TestingWindow AND
  es.AceAssessmentId = rc.AceAssessmentId
LEFT JOIN schools AS sc
ON es.SchoolId = sc.SchoolId
LEFT JOIN students AS st
ON es.StudentUniqueId = st.StudentUniqueId
LEFT JOIN assessments AS a
ON es.AceAssessmentId = a.AceAssessmentId
  