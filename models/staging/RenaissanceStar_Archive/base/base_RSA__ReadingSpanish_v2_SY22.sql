with testing_windows as (
    select * from {{ ref('stg_GSD__RenStarTestingWindows') }}
),

star as (
    select
        case SchoolIdentifier
            when '57b1f93e473b517136000009' then '116814'
            when '57b1f93e473b51713600000b' then '129247'
            when '57b1f93e473b517136000007' then '131656'
            when '061182013023' then '125617'
            else '999999999'
        end as TestedSchoolId,
        NameofInstitution as TestedSchoolName,
        concat(left(SchoolYear, 5), right(SchoolYear, 2)) as SchoolYear,
        StudentRenaissanceID,
        StudentIdentifier,
        StateUniqueId,
        case
            when MiddleName is null then concat(LastSurname, ', ', FirstName)
            else concat(LastSurname, ', ', FirstName, ' ', MiddleName)
        end as DisplayName,
        LastSurname as LastName,
        FirstName,
        MiddleName,
        Gender,
        date(Birthdate) as BirthDate,
        Gradelevel as GradeLevel,
        EnrollmentStatus,
        AssessmentID,
        date(CompletedDateLocal) as AssessmentDate,
        cast(AssessmentNumber as int64) as AssessmentNumber,
        AssessmentType,
        TotalTimeInSeconds,
        GradePlacement,
        Grade as AssessmentGradeLevel,
        GradeEquivalent,
        ScaledScore,
        UnifiedScore,
        PercentileRank,
        NormalCurveEquivalent,
        InstructionalReadingLevel,
        Lexile,
        StudentGrowthPercentileFallFall,
        StudentGrowthPercentileFallSpring,
        StudentGrowthPercentileFallWinter,
        StudentGrowthPercentileSpringSpring,
        StudentGrowthPercentileWinterSpring,
        CurrentSGP,
        cast(right(StateBenchmarkCategoryName, 1) as int64)
            as StateBenchmarkCategoryLevel
    from {{ source('RenaissanceStar_Archive', 'ReadingSpanish_v2_SY22') }}
),

final as (
    select
        s.*,
        case
            when
                s.AssessmentDate between t.AceWindowStartDate and t.AceWindowEndDate
                then t.TestingWindow
        end as AceTestingWindowName,
        case
            when
                s.AssessmentDate between t.AceWindowStartDate and t.AceWindowEndDate
                then t.AceWindowStartDate
        end as AceTestingWindowStartDate,
        case
            when
                s.AssessmentDate between t.AceWindowStartDate and t.AceWindowEndDate
                then t.AceWindowEndDate
        end as AceTestingWindowEndDate,
        t.TestingWindow as StarTestingWindow
    from star as s
    left join testing_windows as t
        on s.SchoolYear = t.SchoolYear
    where
        s.AssessmentDate between t.TestingWindowStartDate and t.TestingWindowEndDate
)

select * from final
