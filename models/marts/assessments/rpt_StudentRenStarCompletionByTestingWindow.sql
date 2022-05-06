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

  student_result_counts AS (
    SELECT * FROM {{ ref('fct_StudentRenStarWindowResultCounts')}}
  ),

  final AS (
    SELECT
      sc.*,
      st.* EXCEPT (SchoolId),
      c.AceAssessmentId,
      a.AssessmentNameShort,
      a.AssessmentSubject,
      c.SchoolYear,
      c.TestingWindow,
      c.ResultCount,
      CASE
        WHEN c.ResultCount > 0 THEN 'Tested'
        WHEN c.ResultCount = 0 THEN 'Not Tested' 
      END AS WindowTestingStatus
    FROM student_result_counts AS c
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
  StudentUniqueId,
  AceAssessmentId,
  SchoolYear,
  TestingWindow



  