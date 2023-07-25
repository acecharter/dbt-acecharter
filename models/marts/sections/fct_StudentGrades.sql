with grades as (
    select * from {{ ref('stg_SP__CourseGrades') }}
    union all
    select * from {{ ref('stg_SPA__CourseGrades_SY23') }}
    union all
    select * from {{ ref('stg_SPA__CourseGrades_SY22') }}
),

final as (
    select
        SchoolYear,
        SchoolId,
        SessionName,
        SectionIdentifier,
        ClassPeriodName,
        StudentUniqueId,
        GradingPeriodDescriptor,
        case
            when GradingPeriodDescriptor = 'First Nine Weeks' then 'Q1'
            when GradingPeriodDescriptor = 'Second Nine Weeks' then 'Q2'
            when GradingPeriodDescriptor = 'Third Nine Weeks' then 'Q3'
            when GradingPeriodDescriptor = 'Fourth Nine Weeks' then 'Q4'
            when GradingPeriodDescriptor = 'First Semester' then 'S1'
            when GradingPeriodDescriptor = 'Second Semester' then 'S2'
        end as GradingPeriod,
        GradeTypeDescriptor,
        IsCurrentCourseEnrollment,
        IsCurrentGradingPeriod,
        NumericGradeEarned,
        LetterGradeEarned
    from grades
    where
        (GradeTypeDescriptor in ('Final', 'Grading Period'))
        or (IsCurrentCourseEnrollment = true and IsCurrentGradingPeriod = true)
)

select distinct * from final
