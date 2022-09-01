WITH
  students AS (
    SELECT * FROM {{ ref('dim_Students') }}
  ),

  schools AS (
      SELECT
        SchoolYear,
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
      sc.* EXCEPT (SchoolYear),
      st.* EXCEPT (SchoolYear,SchoolId),
      c.AssessmentSubject AS StarAssessmentSubject,
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
    AND c.SchoolYear = sc.SchoolYear
    LEFT JOIN students AS st
    ON
      c.StudentUniqueId = st.StudentUniqueId
      AND c.SchoolId = st.SchoolId
      AND c.SchoolYear = st.SchoolYear
    LEFT JOIN assessments AS a
    ON c.AssessmentSubject = a.AssessmentNameShort
  )

SELECT *
FROM final
ORDER BY
  SchoolId,
  DisplayName,
  StarAssessmentSubject,
  SchoolYear,
  TestingPeriod
  