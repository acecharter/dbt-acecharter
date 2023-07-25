select
    case trim(SchoolIdentifier)
        when '57b1f93e473b517136000009' then '116814'
        when '57b1f93e473b51713600000b' then '129247'
        when '57b1f93e473b517136000007' then '131656'
        when '061182013023' then '125617'
        else '999999999'
    end as TestedSchoolId,
    NameofInstitution as TestedSchoolName,
    SchoolYear,
    StudentRenaissanceID,
    cast(StudentIdentifier as string) as StudentIdentifier,
    StateUniqueId,
    case
        when
            MiddleName is null
            then concat(LastSurname, ', ', FirstName)
        else
            concat(
                LastSurname,
                ', ',
                FirstName,
                ' ',
                MiddleName
            )
    end as DisplayName,
    LastSurname as LastName,
    FirstName,
    MiddleName,
    Gender,
    date(BirthDate) as BirthDate,
    GradeLevel,
    EnrollmentStatus,
    AssessmentID,
    date(CompletedDate) as AssessmentDate,
    cast(AssessmentNumber as int64) as AssessmentNumber,
    AssessmentType,
    TotalTimeInSeconds,
    GradePlacement,
    cast(Grade as string) as AssessmentGradeLevel,
    cast(GradeEquivalent as string) as GradeEquivalent,
    ScaledScore,
    UnifiedScore,
    PercentileRank,
    NormalCurveEquivalent,
    Lexile,
    LiteracyClassification,
    StudentGrowthPercentileFallFall,
    StudentGrowthPercentileFallSpring,
    StudentGrowthPercentileFallWinter,
    StudentGrowthPercentileSpringSpring,
    StudentGrowthPercentileWinterSpring,
    CurrentSGP,
    cast(StateBenchmarkCategoryLevel as int64) as StateBenchmarkCategoryLevel
from {{ source('RenaissanceStar_Archive', 'EarlyLiteracy_v2_SY23') }}
