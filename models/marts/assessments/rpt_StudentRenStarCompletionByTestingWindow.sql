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

  completion_by_window AS (
    SELECT * FROM {{ ref('fct_StudentRenStarCompletionByWindow')}}
  ),

  final AS (
    SELECT
      sc.* EXCEPT (SchoolYear),
      st.* EXCEPT (SchoolYear, SchoolId),
      c.AceAssessmentId,
      a.AssessmentNameShort,
      a.AssessmentSubject,
      c.SchoolYear,
      c.StarTestingWindow AS TestingWindow,
      c.* EXCEPT(SchoolYear, StarTestingWindow, AceAssessmentId, SchoolId, StudentUniqueId, AssessmentName, TestingStatus),
      c.TestingStatus AS WindowTestingStatus
    FROM completion_by_window AS c
    LEFT JOIN schools AS sc
    ON
      c.SchoolId = sc.SchoolId
      AND c.SchoolYear = sc.SchoolYear
    LEFT JOIN students AS st
    ON
      c.StudentUniqueId = st.StudentUniqueId
      AND c.SchoolId = st.SchoolId
      AND c.SchoolYear = st.SchoolYear
    LEFT JOIN assessments AS a
    ON c.AceAssessmentId = a.AceAssessmentId
    WHERE st.StudentUniqueId IS NOT NULL
  )

SELECT *
FROM final
ORDER BY
  SchoolId,
  StudentUniqueId,
  AceAssessmentId,
  SchoolYear,
  TestingWindow



  