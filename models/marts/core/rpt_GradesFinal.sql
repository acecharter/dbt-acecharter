SELECT *
FROM {{ ref('stg_StarterPack__CourseGrades') }}
WHERE GradeTypeDescriptor = 'Final'