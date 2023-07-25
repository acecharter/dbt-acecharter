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
from {{ source('RenaissanceStar_Archive', 'Reading_v2_SY22') }}
