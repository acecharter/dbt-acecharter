with reading_english as (
    select * from {{ ref('base_RSA__Reading_SY21')}}
    union all
    select * from {{ ref('base_RSA__Reading_v2_SY22')}}
    union all
    select * from {{ ref('base_RS__Reading_v2')}}
),

reading_spanish as (
    select * from {{ ref('base_RSA__ReadingSpanish_v2_SY22')}}
    union all
    select * from {{ ref('base_RS__ReadingSpanish_v2')}}
),

math_english as (
    select * from {{ ref('base_RSA__Math_SY21')}}
    union all
    select * from {{ ref('base_RSA__Math_v2_SY22')}}
    union all
    select * from {{ ref('base_RS__Math_v2')}}
),

math_spanish as (
    select * from {{ ref('base_RSA__MathSpanish_v2_SY22')}}
    union all
    select * from {{ ref('base_RS__MathSpanish_v2')}}
),

early_literacy_english as (
    select * from {{ ref('base_RSA__EarlyLiteracy_SY21')}}
    union all
    select * from {{ ref('base_RSA__EarlyLiteracy_SY22')}}
    union all
    select * from {{ ref('base_RS__EarlyLiteracy_v2')}}
),

early_literacy_spanish as (
    select * from {{ ref('base_RSA__EarlyLiteracySpanish_v2_SY22')}}
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
        AceAssessmentId,
        AssessmentName,
        AssessmentSubject,
        TestedSchoolId,
        TestedSchoolName,
        SchoolYear,
        StudentRenaissanceID,
        StudentIdentifier,
        StateUniqueId,
        DisplayName,
        LastName,
        FirstName,
        MiddleName,
        Gender,
        BirthDate,
        cast(GradeLevel as int64) as GradeLevel,
        EnrollmentStatus,
        AssessmentID,
        AssessmentDate,
        AssessmentNumber,
        AssessmentType,
        TotalTimeInSeconds,
        GradePlacement,
        cast(AssessmentGradeLevel as int64) as AssessmentGradeLevel,
        GradeEquivalent,
        ScaledScore,
        UnifiedScore,
        PercentileRank,
        NormalCurveEquivalent,
        StudentGrowthPercentileFallFall,
        StudentGrowthPercentileFallSpring,
        StudentGrowthPercentileFallWinter,
        StudentGrowthPercentileSpringSpring,
        StudentGrowthPercentileWinterSpring,
        CurrentSGP,
        StateBenchmarkCategoryLevel,
        AceTestingWindowName,
        AceTestingWindowStartDate,
        AceTestingWindowEndDate,
        StarTestingWindow,
        InstructionalReadingLevel,
        Lexile,
        Quantile,
        LiteracyClassification,
        case
            when substr(GradeEquivalent, 1, 1) = '>' then cast(trim(substr(GradeEquivalent, 2)) as float64) + 0.1
            when substr(GradeEquivalent, 1, 1) = '<' then cast(trim(substr(GradeEquivalent, 2)) as float64) - 0.1
            else cast(GradeEquivalent as float64)
        end as GradeEquivalentNumeric,
        case
            when substr(GradeEquivalent, 1, 1) = '>' then round(cast(trim(substr(GradeEquivalent, 2)) as float64) + 0.1 - GradePlacement, 1)
            when substr(GradeEquivalent, 1, 1) = '<' then round(cast(trim(substr(GradeEquivalent, 2)) as float64) - 0.1 - GradePlacement, 1)
            else round(cast(GradeEquivalent as float64) - GradePlacement, 1)
        end as GradeEquivalentMinusPlacement
    from unioned
)

select * from final