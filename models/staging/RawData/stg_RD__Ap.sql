with unioned as (
  select * from {{ ref('base_RD__Ap2018')}}
  union all
  select * from {{ ref('base_RD__Ap2019')}}
  union all
  select * from {{ ref('base_RD__Ap2020')}}
  union all
  select * from {{ ref('base_RD__Ap2021')}}
  union all
  select * from {{ ref('base_RD__Ap2022')}}
),

final as (
  select
    ApId,
    LastName,
    FirstName,
    MiddleInitial,
    Gender,
    concat(
        case
        when CAST(RIGHT(FORMAT("%06d", DateOfBirth), 2) as INT64) > 90
            then CAST(CAST(RIGHT(FORMAT("%06d", DateOfBirth), 2) as INT64) + 1900 as STRING)
        else CAST(CAST(RIGHT(FORMAT("%06d", DateOfBirth), 2) as INT64) + 2000 as STRING)
        end,
        '-',
        LEFT(FORMAT("%06d", DateOfBirth), 2),
        '-',
        SUBSTR(FORMAT("%06d", DateOfBirth), 3, 2)
    ) as DateOfBirth,
    case
        when GradeLevel = 3 then '<9'
        when GradeLevel = 4 then '9'
        when GradeLevel = 5 then '10'
        when GradeLevel = 6 then  '11'
        when GradeLevel = 7 then '12'
        when GradeLevel = 8 then 'No longer in high school'
        when GradeLevel = 11 then 'Unknown'
    end as GradeLevel,
    AiCode,
    AiInstitutionName,
    AdminYear01,
    ExamCode01,
    ExamGrade01,
    IrregularityCode101,
    IrregularityCode201,
    AdminYear02,
    ExamCode02,
    ExamGrade02,
    IrregularityCode102,
    IrregularityCode202,
    AdminYear03,
    ExamCode03,
    ExamGrade03,
    IrregularityCode103,
    IrregularityCode203,
    AdminYear04,
    ExamCode04,
    ExamGrade04,
    IrregularityCode104,
    IrregularityCode204,
    AdminYear05,
    ExamCode05,
    ExamGrade05,
    IrregularityCode105,
    IrregularityCode205,
    AdminYear06,
    ExamCode06,
    ExamGrade06,
    IrregularityCode106,
    IrregularityCode206,
    AdminYear07,
    ExamCode07,
    ExamGrade07,
    IrregularityCode107,
    IrregularityCode207,
    AdminYear08,
    ExamCode08,
    ExamGrade08,
    IrregularityCode108,
    IrregularityCode208,
    AdminYear09,
    ExamCode09,
    ExamGrade09,
    IrregularityCode109,
    IrregularityCode209,
    AdminYear10,
    ExamCode10,
    ExamGrade10,
    IrregularityCode110,
    IrregularityCode210,
    AdminYear11,
    ExamCode11,
    ExamGrade11,
    IrregularityCode111,
    IrregularityCode211,
    AdminYear12,
    ExamCode12,
    ExamGrade12,
    IrregularityCode112,
    IrregularityCode212,
    AdminYear13,
    ExamCode13,
    ExamGrade13,
    IrregularityCode113,
    IrregularityCode213,
    AdminYear14,
    ExamCode14,
    ExamGrade14,
    IrregularityCode114,
    IrregularityCode214,
    AdminYear15,
    ExamCode15,
    ExamGrade15,
    IrregularityCode115,
    IrregularityCode215,
    AdminYear16,
    ExamCode16,
    ExamGrade16,
    IrregularityCode116,
    IrregularityCode216,
    AdminYear17,
    ExamCode17,
    ExamGrade17,
    IrregularityCode117,
    IrregularityCode217,
    AdminYear18,
    ExamCode18,
    ExamGrade18,
    IrregularityCode118,
    IrregularityCode218,
    AdminYear19,
    ExamCode19,
    ExamGrade19,
    IrregularityCode119,
    IrregularityCode219,
    AdminYear20,
    ExamCode20,
    ExamGrade20,
    IrregularityCode120,
    IrregularityCode220,
    AdminYear21,
    ExamCode21,
    ExamGrade21,
    IrregularityCode121,
    IrregularityCode221,
    AdminYear22,
    ExamCode22,
    ExamGrade22,
    IrregularityCode122,
    IrregularityCode222,
    AdminYear23,
    ExamCode23,
    ExamGrade23,
    IrregularityCode123,
    IrregularityCode223,
    AdminYear24,
    ExamCode24,
    ExamGrade24,
    IrregularityCode124,
    IrregularityCode224,
    AdminYear25,
    ExamCode25,
    ExamGrade25,
    IrregularityCode125,
    IrregularityCode225,
    AdminYear26,
    ExamCode26,
    ExamGrade26,
    IrregularityCode126,
    IrregularityCode226,
    AdminYear27,
    ExamCode27,
    ExamGrade27,
    IrregularityCode127,
    IrregularityCode227,
    AdminYear28,
    ExamCode28,
    ExamGrade28,
    IrregularityCode128,
    IrregularityCode228,
    AdminYear29,
    ExamCode29,
    ExamGrade29,
    IrregularityCode129,
    IrregularityCode229,
    AdminYear30,
    ExamCode30,
    ExamGrade30,
    IrregularityCode130,
    IrregularityCode230,
    StudentIdentifier,
    case
        when RaceEthnicity = 0 then 'No Response'
        when RaceEthnicity = 1 then 'American Indian/Alaska Native'
        when RaceEthnicity = 2 then 'Asian'
        when RaceEthnicity = 3 then 'Black/African American'
        when RaceEthnicity = 4 then 'Hispanic/Latino'
        when RaceEthnicity = 8 then 'Native Hawaiian or Other Pacific Islander'
        when RaceEthnicity = 9 then 'White'
        when RaceEthnicity = 10 then 'Other'
        when RaceEthnicity = 12 then 'Two or More Races, Non-Hispanic'
    end as RaceEthnicity
  from unioned
)

select * from final