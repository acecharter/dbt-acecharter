with final as (
    select
        case trim(SchoolIdentifier)
            when '57b1f93e473b517136000009' then '116814'
            when '57b1f93e473b51713600000b' then '129247'
            when '57b1f93e473b517136000007' then '131656'
            when '061182013023' then '125617'
            else '999999999'
        end as TestedSchoolId,
        SchoolName as TestedSchoolName,
        concat(left(SchoolYear, 4), '-', right(SchoolYear, 2)) as SchoolYear,
        StudentRenaissanceID,
        cast(StudentIdentifier as string) as StudentIdentifier,
        cast(StudentStateID as string) as StateUniqueId,
        case
            when
                StudentMiddleName is null
                then concat(StudentLastName, ', ', StudentFirstName)
            else
                concat(
                    StudentLastName,
                    ', ',
                    StudentFirstName,
                    ' ',
                    StudentMiddleName
                )
        end as DisplayName,
        StudentLastName as LastName,
        StudentFirstName as FirstName,
        StudentMiddleName as MiddleName,
        Gender,
        date(BirthDate) as BirthDate,
        cast(CurrentGrade as string) as GradeLevel,
        EnrollmentStatus,
        AssessmentID,
        date(CompletedDate) as AssessmentDate,
        AssessmentNumber,
        AssessmentType,
        TotalTimeInSeconds,
        GradePlacement,
        cast(Grade as string) as AssessmentGradeLevel,
        cast(GradeEquivalent as string) as GradeEquivalent,
        ScaledScore,
        UnifiedScore,
        cast(null as int64) as PercentileRank,
        cast(null as float64) as NormalCurveEquivalent,
        cast(null as string) as Lexile,
        LiteracyClassification,
        cast(null as int64) as StudentGrowthPercentileFallFall,
        cast(null as int64) as StudentGrowthPercentileFallSpring,
        cast(null as int64) as StudentGrowthPercentileFallWinter,
        cast(null as int64) as StudentGrowthPercentileSpringSpring,
        cast(null as int64) as StudentGrowthPercentileWinterSpring,
        cast(null as int64) as CurrentSGP,
        cast(null as int64) as StateBenchmarkCategoryLevel,
        cast(null as string) as AceTestingWindowName,
        cast(null as date) as AceTestingWindowStartDate,
        cast(null as date) as AceTestingWindowEndDate,
        case
            when CompletedDate between '2021-08-01' and '2021-11-30' then 'Fall'
            when
                CompletedDate between '2021-12-01' and '2022-03-31'
                then 'Winter'
            when
                CompletedDate between '2022-04-01' and '2022-07-31'
                then 'Spring'
        end as StarTestingWindow
    from {{ source('RenaissanceStar_Archive', 'EarlyLiteracy_SY22') }}
)

select * from final
