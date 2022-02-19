WITH
  course_enrollments AS (
    SELECT * FROM {{ ref('dim_CourseEnrollments')}}
  ),

  course_grades AS (
    SELECT * FROM {{ ref('fct_CourseGrades')}}
  ),

  schools AS (
    SELECT * FROM {{ref('dim_Schools')}}
  ),

  students AS (
    SELECT *
  FROM {{ ref('dim_Students') }}
  ),

  final AS (
    SELECT
      sc.SchoolName,
      sc.SchoolNameMid,
      sc.SchoolNameShort,
      e.* EXCEPT(CourseLevelCharacteristic),
      g.* EXCEPT(
        SchoolId,
        SessionName,
        SectionIdentifier,
        ClassPeriodName,
        StudentUniqueId
      ),
      st.* EXCEPT(SchoolId, StudentUniqueId)
    FROM course_enrollments AS e
    LEFT JOIN course_grades AS g
    ON
      e.SchoolId = g.SchoolId
      AND e.SessionName = g.SessionName
      AND e.SectionIdentifier = g.SectionIdentifier
      AND e.ClassPeriodName = g.ClassPeriodName
      AND e.StudentUniqueId = g.StudentUniqueId
    LEFT JOIN schools AS sc
    ON e.SchoolId = sc.SchoolId
    LEFT JOIN students AS st
    ON
      e.SchoolId = st.SchoolId
      AND e.StudentUniqueId = st.StudentUniqueId
  )

SELECT * FROM final