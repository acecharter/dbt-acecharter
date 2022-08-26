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

  completion_by_window AS (
    SELECT * FROM {{ ref('fct_StudentRenStarCompletionByWindow')}}
    WHERE SchoolYear = '2022-23'
  ),

  final AS (
    SELECT
      sc.*,
      st.* EXCEPT (SchoolId),
      c.AceAssessmentId,
      a.AssessmentNameShort,
      a.AssessmentSubject,
      c.SchoolYear,
      c.StarTestingWindow AS TestingWindow,
      c.* EXCEPT(SchoolYear, StarTestingWindow, AceAssessmentId, SchoolId, StudentUniqueId, AssessmentName, TestingStatus),
      c.TestingStatus AS WindowTestingStatus
    FROM completion_by_window AS c
    LEFT JOIN schools AS sc
    ON c.SchoolId = sc.SchoolId
    LEFT JOIN students AS st
    ON
      c.StudentUniqueId = st.StudentUniqueId
      AND c.SchoolId = st.SchoolId
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



  