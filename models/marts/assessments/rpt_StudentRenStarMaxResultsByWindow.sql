WITH
  schools AS (
    SELECT DISTINCT
      SchoolId,
      SchoolName,
      SchoolNameMid,
      SchoolNameShort
    FROM {{ ref('dim_Schools')}}
  ),

  students AS (
    SELECT * FROM {{ ref('dim_Students') }}
  ),

  star_results AS (
    SELECT
      r.AceAssessmentId,
      r.AssessmentName,
      r.StateUniqueId,
      r.TestedSchoolId,
      CASE
        WHEN r.TestedSchoolId = s.SchoolId THEN s.SchoolNameShort
        ELSE 'Non-ACE School'
      END AS TestedSchoolName,
      r.SchoolYear AS AssessmentSchoolYear,
      r.StarTestingWindow,
      r.AssessmentGradeLevel,
      r.AssessmentSubject,
      r.ReportingMethod,
      MAX(CAST(r.StudentResult AS FLOAT64)) AS MaxStudentResult
    FROM {{ ref('int_RenStar_unpivoted')}} AS r
    LEFT JOIN schools AS s
    ON r.TestedSchoolId = s.SchoolId
    WHERE StudentResultDataType IN ('INT64', 'FLOAT64')
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
  ),

  final AS (
    SELECT
      s.* EXCEPT (SchoolId),
      st.* EXCEPT (SchoolYear),
      r.* EXCEPT (StateUniqueId)
    FROM students AS st
    INNER JOIN star_results AS r
    ON st.StateUniqueId = r.StateUniqueId
    AND st.SchoolYear = r.AssessmentSchoolYear
    INNER JOIN schools AS s
    ON st.SchoolId = s.SchoolId
  )

SELECT * FROM final
