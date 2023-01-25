with unioned as (
  select * from {{ ref('baseRDAp2018')}}
  union all
  select * from {{ ref('baseRDAnet2019')}}
  union all
  select * from {{ ref('baseRDAnet2020')}}
  union all
  select * from {{ ref('baseRDAnet2021')}}
  union all
  select * from {{ ref('baseRDAnet2022')}}
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
  CONCAT('20', CAST(AdminYear01 as STRING)) as AdminYear01,
  case
    when CAST(ExamCode01 as STRING) = '7' then 'United States History'
    when CAST(ExamCode01 as STRING) = '13' then 'Art History'
    when CAST(ExamCode01 as STRING) = '14' then 'Drawing'
    when CAST(ExamCode01 as STRING) = '15' then '2-D Art and Design'
    when CAST(ExamCode01 as STRING) = '16' then '3-D Art and Design'
    when CAST(ExamCode01 as STRING) = '20' then 'Biology'
    when CAST(ExamCode01 as STRING) = '22' then 'Seminar'
    when CAST(ExamCode01 as STRING) = '23' then 'Research'
    when CAST(ExamCode01 as STRING) = '25' then 'Chemistry'
    when CAST(ExamCode01 as STRING) = '28' then 'Chinese Language and Culture'
    when CAST(ExamCode01 as STRING) = '31' then 'Computer Science A'
    when CAST(ExamCode01 as STRING) = '32' then 'Computer Science Principles'
    when CAST(ExamCode01 as STRING) = '33' then 'Computer Science AB'
    when CAST(ExamCode01 as STRING) = '34' then 'Microeconomics'
    when CAST(ExamCode01 as STRING) = '35' then 'Macroeconomics'
    when CAST(ExamCode01 as STRING) = '36' then 'English Language and Composition'
    when CAST(ExamCode01 as STRING) = '37' then 'English Literature and Composition'
    when CAST(ExamCode01 as STRING) = '40' then 'Environmental Science'
    when CAST(ExamCode01 as STRING) = '43' then 'European History'
    when CAST(ExamCode01 as STRING) = '48' then 'French Language and Culture'
    when CAST(ExamCode01 as STRING) = '51' then 'French Literature'
    when CAST(ExamCode01 as STRING) = '53' then 'Human Geography'
    when CAST(ExamCode01 as STRING) = '55' then 'German Language and Culture'
    when CAST(ExamCode01 as STRING) = '57' then 'United States Government and Politics'
    when CAST(ExamCode01 as STRING) = '58' then 'Comparative Government and Politics'
    when CAST(ExamCode01 as STRING) = '60' then 'Latin'
    when CAST(ExamCode01 as STRING) = '61' then 'Latin Literature'
    when CAST(ExamCode01 as STRING) = '62' then 'Italian Language and Culture'
    when CAST(ExamCode01 as STRING) = '64' then 'Japanese Language and Culture'
    when CAST(ExamCode01 as STRING) = '66' then 'Calculus AB'
    when CAST(ExamCode01 as STRING) = '68' then 'Calculus BC'
    when CAST(ExamCode01 as STRING) = '69' then 'Calculus BC: AB Structure'
    when CAST(ExamCode01 as STRING) = '75' then 'Music Theory'
    when CAST(ExamCode01 as STRING) = '76' then 'Music Aural Subscore'
    when CAST(ExamCode01 as STRING) = '77' then 'Music Non-Aural Subscore'
    when CAST(ExamCode01 as STRING) = '78' then 'Physics B'
    when CAST(ExamCode01 as STRING) = '80' then 'Physics C: Mechanics'
    when CAST(ExamCode01 as STRING) = '82' then 'Physics C: Electricity and Magnetism'
    when CAST(ExamCode01 as STRING) = '83' then 'Physics 1'
    when CAST(ExamCode01 as STRING) = '84' then 'Physics 2'
    when CAST(ExamCode01 as STRING) = '85' then 'Psychology'
    when CAST(ExamCode01 as STRING) = '87' then 'Spanish Language and Culture'
    when CAST(ExamCode01 as STRING) = '89' then 'Spanish Literature and Culture'
    when CAST(ExamCode01 as STRING) = '90' then 'Statistics'
    when CAST(ExamCode01 as STRING) = '93' then 'World History: Modern'
  end as ExamCode01,
  CAST(ExamGrade01 as STRING) as ExamGrade01,
  CAST(IrregularityCode101 as STRING) as IrregularityCode101,
  CAST(IrregularityCode201 as STRING) as IrregularityCode201,
  CAST(AdminYear02 as STRING) as AdminYear02,
  CAST(ExamCode02 as STRING) as ExamCode02,
  CAST(ExamGrade02 as STRING) as ExamGrade02,
  CAST(IrregularityCode102 as STRING) as IrregularityCode102,
  CAST(IrregularityCode202 as STRING) as IrregularityCode202,
  CAST(AdminYear03 as STRING) as AdminYear03,
  CAST(ExamCode03 as STRING) as ExamCode03,
  CAST(ExamGrade03 as STRING) as ExamGrade03,
  CAST(IrregularityCode103 as STRING) as IrregularityCode103,
  CAST(IrregularityCode203 as STRING) as IrregularityCode203,
  CAST(AdminYear04 as STRING) as AdminYear04,
  CAST(ExamCode04 as STRING) as ExamCode04,
  CAST(ExamGrade04 as STRING) as ExamGrade04,
  CAST(IrregularityCode104 as STRING) as IrregularityCode104,
  CAST(IrregularityCode204 as STRING) as IrregularityCode204,
  CAST(AdminYear05 as STRING) as AdminYear05,
  CAST(ExamCode05 as STRING) as ExamCode05,
  CAST(ExamGrade05 as STRING) as ExamGrade05,
  CAST(IrregularityCode105 as STRING) as IrregularityCode105,
  CAST(IrregularityCode205 as STRING) as IrregularityCode205,
  CAST(AdminYear06 as STRING) as AdminYear06,
  CAST(ExamCode06 as STRING) as ExamCode02,
  CAST(ExamGrade06 as STRING) as ExamGrade02,
  CAST(IrregularityCode106 as STRING) as IrregularityCode102,
  CAST(IrregularityCode206 as STRING) as IrregularityCode202,
  CAST(AdminYear07 as STRING) as AdminYear07,
  CAST(ExamCode07 as STRING) as ExamCode07,
  CAST(ExamGrade07 as STRING) as ExamGrade07,
  CAST(IrregularityCode107 as STRING) as IrregularityCode107,
  CAST(IrregularityCode207 as STRING) as IrregularityCode207,
  CAST(AdminYear08 as STRING) as AdminYear08,
  CAST(ExamCode08 as STRING) as ExamCode08,
  CAST(ExamGrade08 as STRING) as ExamGrade08,
  CAST(IrregularityCode108 as STRING) as IrregularityCode108,
  CAST(IrregularityCode208 as STRING) as IrregularityCode208,
  CAST(AdminYear09 as STRING) as AdminYear09,
  CAST(ExamCode09 as STRING) as ExamCode09,
  CAST(ExamGrade09 as STRING) as ExamGrade09,
  CAST(IrregularityCode109 as STRING) as IrregularityCode109,
  CAST(IrregularityCode209 as STRING) as IrregularityCode209,
  CAST(AdminYear10 as STRING) as AdminYear10,
  CAST(ExamCode10 as STRING) as ExamCode10,
  CAST(ExamGrade10 as STRING) as ExamGrade10,
  CAST(IrregularityCode110 as STRING) as IrregularityCode110,
  CAST(IrregularityCode210 as STRING) as IrregularityCode210,
  CAST(AdminYear11 as STRING) as AdminYear11,
  CAST(ExamCode11 as STRING) as ExamCode11,
  CAST(ExamGrade11 as STRING) as ExamGrade11,
  CAST(IrregularityCode111 as STRING) as IrregularityCode111,
  CAST(IrregularityCode211 as STRING) as IrregularityCode211,
  CAST(AdminYear12 as STRING) as AdminYear12,
  CAST(ExamCode12 as STRING) as ExamCode12,
  CAST(ExamGrade12 as STRING) as ExamGrade12,
  CAST(IrregularityCode112 as STRING) as IrregularityCode112,
  CAST(IrregularityCode212 as STRING) as IrregularityCode212,
  CAST(AdminYear13 as STRING) as AdminYear13,
  CAST(ExamCode13 as STRING) as ExamCode13,
  CAST(ExamGrade13 as STRING) as ExamGrade13,
  CAST(IrregularityCode113 as STRING) as IrregularityCode113,
  CAST(IrregularityCode213 as STRING) as IrregularityCode213,
  CAST(AdminYear14 as STRING) as AdminYear14,
  CAST(ExamCode14 as STRING) as ExamCode14,
  CAST(ExamGrade14 as STRING) as ExamGrade14,
  CAST(IrregularityCode114 as STRING) as IrregularityCode114,
  CAST(IrregularityCode214 as STRING) as IrregularityCode214,
  CAST(AdminYear15 as STRING) as AdminYear15,
  CAST(ExamCode15 as STRING) as ExamCode15,
  CAST(ExamGrade15 as STRING) as ExamGrade15,
  CAST(IrregularityCode115 as STRING) as IrregularityCode115,
  CAST(IrregularityCode215 as STRING) as IrregularityCode215,
  CAST(AdminYear16 as STRING) as AdminYear16,
  CAST(ExamCode16 as STRING) as ExamCode16,
  CAST(ExamGrade16 as STRING) as ExamGrade16,
  CAST(IrregularityCode116 as STRING) as IrregularityCode116,
  CAST(IrregularityCode216 as STRING) as IrregularityCode216,
  CAST(AdminYear17 as STRING) as AdminYear17,
  CAST(ExamCode17 as STRING) as ExamCode17,
  CAST(ExamGrade17 as STRING) as ExamGrade17,
  CAST(IrregularityCode117 as STRING) as IrregularityCode117,
  CAST(IrregularityCode217 as STRING) as IrregularityCode217,
  CAST(AdminYear18 as STRING) as AdminYear18,
  CAST(ExamCode18 as STRING) as ExamCode18,
  CAST(ExamGrade18 as STRING) as ExamGrade18,
  CAST(IrregularityCode118 as STRING) as IrregularityCode118,
  CAST(IrregularityCode218 as STRING) as IrregularityCode218,
  CAST(AdminYear19 as STRING) as AdminYear19,
  CAST(ExamCode19 as STRING) as ExamCode19,
  CAST(ExamGrade19 as STRING) as ExamGrade19,
  CAST(IrregularityCode119 as STRING) as IrregularityCode119,
  CAST(IrregularityCode219 as STRING) as IrregularityCode219,
  CAST(AdminYear20 as STRING) as AdminYear20,
  CAST(ExamCode20 as STRING) as ExamCode20,
  CAST(ExamGrade20 as STRING) as ExamGrade20,
  CAST(IrregularityCode120 as STRING) as IrregularityCode120,
  CAST(IrregularityCode220 as STRING) as IrregularityCode220,
  CAST(AdminYear21 as STRING) as AdminYear21,
  CAST(ExamCode21 as STRING) as ExamCode21,
  CAST(ExamGrade21 as STRING) as ExamGrade21,
  CAST(IrregularityCode121 as STRING) as IrregularityCode121,
  CAST(IrregularityCode221 as STRING) as IrregularityCode221,
  CAST(AdminYear22 as STRING) as AdminYear22,
  CAST(ExamCode22 as STRING) as ExamCode22,
  CAST(ExamGrade22 as STRING) as ExamGrade22,
  CAST(IrregularityCode122 as STRING) as IrregularityCode122,
  CAST(IrregularityCode222 as STRING) as IrregularityCode222,
  CAST(AdminYear23 as STRING) as AdminYear23,
  CAST(ExamCode23 as STRING) as ExamCode23,
  CAST(ExamGrade23 as STRING) as ExamGrade23,
  CAST(IrregularityCode123 as STRING) as IrregularityCode123,
  CAST(IrregularityCode223 as STRING) as IrregularityCode223,
  CAST(AdminYear24 as STRING) as AdminYear24,
  CAST(ExamCode24 as STRING) as ExamCode24,
  CAST(ExamGrade24 as STRING) as ExamGrade24,
  CAST(IrregularityCode124 as STRING) as IrregularityCode124,
  CAST(IrregularityCode224 as STRING) as IrregularityCode224,
  CAST(AdminYear25 as STRING) as AdminYear25,
  CAST(ExamCode25 as STRING) as ExamCode25,
  CAST(ExamGrade25 as STRING) as ExamGrade25,
  CAST(IrregularityCode125 as STRING) as IrregularityCode125,
  CAST(IrregularityCode225 as STRING) as IrregularityCode225,
  CAST(AdminYear26 as STRING) as AdminYear26,
  CAST(ExamCode26 as STRING) as ExamCode26,
  CAST(ExamGrade26 as STRING) as ExamGrade26,
  CAST(IrregularityCode126 as STRING) as IrregularityCode126,
  CAST(IrregularityCode226 as STRING) as IrregularityCode226,
  CAST(AdminYear27 as STRING) as AdminYear27,
  CAST(ExamCode27 as STRING) as ExamCode27,
  CAST(ExamGrade27 as STRING) as ExamGrade27,
  CAST(IrregularityCode127 as STRING) as IrregularityCode127,
  CAST(IrregularityCode227 as STRING) as IrregularityCode227,
  CAST(AdminYear28 as STRING) as AdminYear28,
  CAST(ExamCode28 as STRING) as ExamCode28,
  CAST(ExamGrade28 as STRING) as ExamGrade28,
  CAST(IrregularityCode128 as STRING) as IrregularityCode128,
  CAST(IrregularityCode228 as STRING) as IrregularityCode228,
  CAST(AdminYear29 as STRING) as AdminYear29,
  CAST(ExamCode29 as STRING) as ExamCode29,
  CAST(ExamGrade29 as STRING) as ExamGrade29,
  CAST(IrregularityCode129 as STRING) as IrregularityCode129,
  CAST(IrregularityCode229 as STRING) as IrregularityCode229,
  CAST(AdminYear30 as STRING) as AdminYear30,
  CAST(ExamCode30 as STRING) as ExamCode30,
  CAST(ExamGrade30 as STRING) as ExamGrade30,
  CAST(IrregularityCode130 as STRING) as IrregularityCode130,
  CAST(IrregularityCode230 as STRING) as IrregularityCode230,
  CAST(StudentIdentifier as STRING) as StateUniqueId,
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
FROM unioned
)

select * from final