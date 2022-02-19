WITH
  courses AS (
    SELECT
      CourseCode,
      CourseTitle,
      CourseLevelCharacteristic,
      CourseGpaApplicability
    FROM {{ ref('stg_SP__CourseEnrollments_v2') }}
    GROUP BY 1, 2, 3, 4
    ORDER BY 1
  ),

  academic_subjects AS (
    SELECT
      CourseCode,
      AcademicSubject
    FROM {{ ref('stg_GSD__CourseSubjects')}}
  ),

  final AS (
    SELECT
      c.*,
      s.AcademicSubject
    FROM courses AS c
    LEFT JOIN academic_subjects AS s
    USING (CourseCode)
  )


SELECT * FROM final
