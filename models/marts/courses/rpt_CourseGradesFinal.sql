SELECT *
FROM {{ ref('rpt_CourseGrades')}}
WHERE GradeTypeDescriptor = 'Final'