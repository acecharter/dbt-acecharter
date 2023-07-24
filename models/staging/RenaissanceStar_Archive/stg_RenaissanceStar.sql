with testing_windows as (
    select * from {{ ref('stg_GSD__RenStarTestingWindows') }}
),

reading_english as (
    select * from {{ ref('base_RSA__Reading_SY21')}}
    union all
    select * from {{ ref('base_RSA__Reading_v2_SY22')}}
    union all
    select * from {{ ref('base_RSA__Reading_v2_SY23')}}
    union all
    select * from {{ ref('base_RS__Reading_v2')}}
),

reading_spanish as (
    select * from {{ ref('base_RSA__ReadingSpanish_v2_SY22')}}
    union all
    select * from {{ ref('base_RSA__ReadingSpanish_v2_SY23')}}
    union all
    select * from {{ ref('base_RS__ReadingSpanish_v2')}}
),

math_english as (
    select * from {{ ref('base_RSA__Math_SY21')}}
    union all
    select * from {{ ref('base_RSA__Math_v2_SY22')}}
    union all
    select * from {{ ref('base_RSA__Math_v2_SY23')}}
    union all
    select * from {{ ref('base_RS__Math_v2')}}
),

math_spanish as (
    select * from {{ ref('base_RSA__MathSpanish_v2_SY22')}}
    union all
    select * from {{ ref('base_RSA__MathSpanish_v2_SY23')}}
    union all
    select * from {{ ref('base_RS__MathSpanish_v2')}}
),

early_literacy_english as (
    select * from {{ ref('base_RSA__EarlyLiteracy_SY21')}}
    union all
    select * from {{ ref('base_RSA__EarlyLiteracy_SY22')}}
    union all
    select * from {{ ref('base_RSA__EarlyLiteracy_v2_SY23')}}
    union all
    select * from {{ ref('base_RS__EarlyLiteracy_v2')}}
),

early_literacy_spanish as (
    select * from {{ ref('base_RSA__EarlyLiteracySpanish_v2_SY22')}}
    union all
    select * from {{ ref('base_RSA__EarlyLiteracySpanish_v2_SY23')}}
    union all
    select * from {{ ref('base_RS__EarlyLiteracySpanish_v2')}}
),

reading_all as (
    select
        case
            when AssessmentType like '%Enterprise%' then '11'
            when AssessmentType = 'ProgressMonitoring' then '24'
        end as AceAssessmentId,
        case
            when AssessmentType like '%Enterprise%' then 'Star Reading'
            when AssessmentType = 'ProgressMonitoring' then 'Star Reading Progress Monitoring'
        end as AssessmentName,
        'Reading' as AssessmentSubject,
        *
    from reading_english

    union all

    select
        '13' as AceAssessmentId,
        'Star Reading (Spanish)' as AssessmentName,
        'Reading (Spanish)' as AssessmentSubject,
        *
    from reading_spanish
),

math_all as (
    select
        case
            when AssessmentType like '%Enterprise%' then '10'
            when AssessmentType = 'ProgressMonitoring' then '23'
        end as AceAssessmentId,
        case
            when AssessmentType like '%Enterprise%' then 'Star Math'
            when AssessmentType = 'ProgressMonitoring' then 'Star Math Progress Monitoring'
        end as AssessmentName,
        'Math' as AssessmentSubject,
        *
    from math_english

    union all

    select
        '12' as AceAssessmentId,
        'Star Math (Spanish)' as AssessmentName,
        'Math (Spanish)' as AssessmentSubject,
        *
    from math_spanish
),

early_literacy_all as (
    select
        '21' as AceAssessmentId,
        'Star Early Literacy' as AssessmentName,
        'Early Literacy' as AssessmentSubject,
        *
    from early_literacy_english

    union all
    
    select
        '22' as AceAssessmentId,
        'Star Early Literacy (Spanish)' as AssessmentName,
        'Early Literacy (Spanish)' as AssessmentSubject,
        *
    from early_literacy_english
),

reading as (
    select
        * except(
            InstructionalReadingLevel, 
            Lexile
        ),
        InstructionalReadingLevel,
        Lexile,
        cast(null as string) as Quantile,
        cast(null as string) as LiteracyClassification
    from reading_all
),

math as (
    select
        * except(Quantile),
        cast(null as string) as InstructionalReadingLevel,
        cast(null as string) as Lexile,
        Quantile,
        cast(null as string) as LiteracyClassification
    from math_all
),

early_literacy as (
    select
        * except(Lexile, LiteracyClassification),
        cast(null as string) as InstructionalReadingLevel,
        Lexile,
        cast(null as string) as Quantile,
        LiteracyClassification
    from early_literacy_all
),

unioned as (
    select * from reading
    union all
    select * from math
    union all
    select * from early_literacy
),

final as (
    select
        u.AceAssessmentId,
        u.AssessmentName,
        u.AssessmentSubject,
        u.TestedSchoolId,
        u.TestedSchoolName,
        u.SchoolYear,
        u.StudentRenaissanceID,
        u.StudentIdentifier,
        u.StateUniqueId,
        u.DisplayName,
        u.LastName,
        u.FirstName,
        u.MiddleName,
        u.Gender,
        u.BirthDate,
        cast(u.GradeLevel as int64) as GradeLevel,
        u.EnrollmentStatus,
        u.AssessmentID,
        u.AssessmentDate,
        u.AssessmentNumber,
        u.AssessmentType,
        u.TotalTimeInSeconds,
        u.GradePlacement,
        cast(u.AssessmentGradeLevel as int64) as AssessmentGradeLevel,
        u.GradeEquivalent,
        u.ScaledScore,
        u.UnifiedScore,
        u.PercentileRank,
        u.NormalCurveEquivalent,
        u.StudentGrowthPercentileFallFall,
        u.StudentGrowthPercentileFallSpring,
        u.StudentGrowthPercentileFallWinter,
        u.StudentGrowthPercentileSpringSpring,
        u.StudentGrowthPercentileWinterSpring,
        u.CurrentSGP,
        u.StateBenchmarkCategoryLevel,
        case
            when
                u.AssessmentDate between t.AceWindowStartDate and t.AceWindowEndDate
                then t.TestingWindow
        end as AceTestingWindowName,
        case
            when
                u.AssessmentDate between t.AceWindowStartDate and t.AceWindowEndDate
                then t.AceWindowStartDate
        end as AceTestingWindowStartDate,
        case
            when
                u.AssessmentDate between t.AceWindowStartDate and t.AceWindowEndDate
                then t.AceWindowEndDate
        end as AceTestingWindowEndDate,
        case
            when u.AssessmentDate between
                date(
                    concat(substr(u.SchoolYear, 1, 4), '-08-01')
                )
                and date(
                    concat(substr(u.SchoolYear, 1, 4), '-11-30')
                )
                then 'Fall'
            when u.AssessmentDate between
                date(
                    concat(substr(u.SchoolYear, 1, 4),'-12-01')
                )
                and date(
                    concat('20', substr(u.SchoolYear, 6, 2), '-03-31')
                )
                then 'Winter'
            when u.AssessmentDate between 
                date(
                    concat('20', substr(u.SchoolYear, 6, 2), '-04-01')
                )
                and date(
                    concat('20', substr(u.SchoolYear, 6, 2), '-07-31')
                )
                then 'Spring'
            else 'ERROR'
        end as StarTestingWindow,
        u.InstructionalReadingLevel,
        u.Lexile,
        u.Quantile,
        u.LiteracyClassification,
        case
            when substr(u.GradeEquivalent, 1, 1) = '>' then cast(trim(substr(u.GradeEquivalent, 2)) as float64) + 0.1
            when substr(u.GradeEquivalent, 1, 1) = '<' then cast(trim(substr(u.GradeEquivalent, 2)) as float64) - 0.1
            else cast(u.GradeEquivalent as float64)
        end as GradeEquivalentNumeric,
        case
            when substr(u.GradeEquivalent, 1, 1) = '>' then round(cast(trim(substr(u.GradeEquivalent, 2)) as float64) + 0.1 - u.GradePlacement, 1)
            when substr(u.GradeEquivalent, 1, 1) = '<' then round(cast(trim(substr(u.GradeEquivalent, 2)) as float64) - 0.1 - u.GradePlacement, 1)
            else round(cast(u.GradeEquivalent as float64) - u.GradePlacement, 1)
        end as GradeEquivalentMinusPlacement
    from unioned as u
    left join testing_windows as t
        on u.SchoolYear = t.SchoolYear
    where u.AssessmentDate between t.TestingWindowStartDate and t.TestingWindowEndDate
)

select distinct * from final
