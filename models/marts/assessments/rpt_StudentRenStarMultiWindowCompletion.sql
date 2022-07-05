WITH
  students AS (
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

  student_completion AS (
    SELECT * FROM {{ ref('fct_StudentRenStarMultiWindowCompletion')}}
  ),

  final AS (
    SELECT
      sc.*,
      st.* EXCEPT (SchoolId),
      c.AceAssessmentId,
      a.AssessmentNameShort,
      a.AssessmentSubject,
      c.SchoolYear,
      c.TestingPeriod,
      c.PreTestStatus,
      c.PostTestStatus,
      c.CompletedPreAndPostTest,
      c.IncludeInPeriodCompletionRate
    FROM student_completion AS c
    LEFT JOIN schools AS sc
    ON c.SchoolId = sc.SchoolId
    LEFT JOIN students AS st
    ON
      c.StudentUniqueId = st.StudentUniqueId
      AND c.SchoolId = st.SchoolId
    LEFT JOIN assessments AS a
    ON c.AceAssessmentId = a.AceAssessmentId
  )

SELECT *
FROM final
ORDER BY
  SchoolId,
  DisplayName,
  AceAssessmentId,
  SchoolYear,
  TestingPeriod
  