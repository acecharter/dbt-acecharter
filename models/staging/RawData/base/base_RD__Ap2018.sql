select
    2018 as SourceFileYear,
    cast(_AP_Number___AP_ID_ as string) as ApId,
    Last_Name as LastName,
    First_Name as FirstName,
    Middle_Initial as MiddleInitial,
    Gender,
    concat(
        case
            when cast(right(format('%06d', Date_Of_Birth), 2) as int64) > 90
                then
                    cast(
                        cast(right(format('%06d', Date_Of_Birth), 2) as int64)
                        + 1900 as string
                    )
            else
                cast(
                    cast(right(format('%06d', Date_Of_Birth), 2) as int64)
                    + 2000 as string
                )
        end,
        '-',
        left(format('%06d', Date_Of_Birth), 2),
        '-',
        substr(format('%06d', Date_Of_Birth), 3, 2)
    ) as DateOfBirth,
    case Grade_Level
        when 3 then '<9'
        when 4 then '9'
        when 5 then '10'
        when 6 then '11'
        when 7 then '12'
        when 8 then 'No longer in high school'
        when 11 then 'Unknown'
    end as GradeLevel,
    AI_Code as AiCode,
    AI_Institution_Name as AiInstitutionName,
    cast(Admin_Year_01 as string) as AdminYear01,
    cast(Exam_Code_01 as string) as ExamCode01,
    cast(Exam_Grade_01 as string) as ExamGrade01,
    cast(Irregularity_Code__1_01 as string) as IrregularityCode101,
    cast(Irregularity_Code__2_01 as string) as IrregularityCode201,
    cast(Admin_Year_02 as string) as AdminYear02,
    cast(Exam_Code_02 as string) as ExamCode02,
    cast(Exam_Grade_02 as string) as ExamGrade02,
    cast(Irregularity_Code__1_02 as string) as IrregularityCode102,
    cast(Irregularity_Code__2_02 as string) as IrregularityCode202,
    cast(Admin_Year_03 as string) as AdminYear03,
    cast(Exam_Code_03 as string) as ExamCode03,
    cast(Exam_Grade_03 as string) as ExamGrade03,
    cast(Irregularity_Code__1_03 as string) as IrregularityCode103,
    cast(Irregularity_Code__2_03 as string) as IrregularityCode203,
    cast(Admin_Year_04 as string) as AdminYear04,
    cast(Exam_Code_04 as string) as ExamCode04,
    cast(Exam_Grade_04 as string) as ExamGrade04,
    cast(Irregularity_Code__1_04 as string) as IrregularityCode104,
    cast(Irregularity_Code__2_04 as string) as IrregularityCode204,
    cast(Admin_Year_05 as string) as AdminYear05,
    cast(Exam_Code_05 as string) as ExamCode05,
    cast(Exam_Grade_05 as string) as ExamGrade05,
    cast(Irregularity_Code__1_05 as string) as IrregularityCode105,
    cast(Irregularity_Code__2_05 as string) as IrregularityCode205,
    cast(Admin_Year_06 as string) as AdminYear06,
    cast(Exam_Code_06 as string) as ExamCode06,
    cast(Exam_Grade_06 as string) as ExamGrade06,
    cast(Irregularity_Code__1_06 as string) as IrregularityCode106,
    cast(Irregularity_Code__2_06 as string) as IrregularityCode206,
    cast(Admin_Year_07 as string) as AdminYear07,
    cast(Exam_Code_07 as string) as ExamCode07,
    cast(Exam_Grade_07 as string) as ExamGrade07,
    cast(Irregularity_Code__1_07 as string) as IrregularityCode107,
    cast(Irregularity_Code__2_07 as string) as IrregularityCode207,
    cast(Admin_Year_08 as string) as AdminYear08,
    cast(Exam_Code_08 as string) as ExamCode08,
    cast(Exam_Grade_08 as string) as ExamGrade08,
    cast(Irregularity_Code__1_08 as string) as IrregularityCode108,
    cast(Irregularity_Code__2_08 as string) as IrregularityCode208,
    cast(Admin_Year_09 as string) as AdminYear09,
    cast(Exam_Code_09 as string) as ExamCode09,
    cast(Exam_Grade_09 as string) as ExamGrade09,
    cast(Irregularity_Code__1_09 as string) as IrregularityCode109,
    cast(Irregularity_Code__2_09 as string) as IrregularityCode209,
    cast(Admin_Year_10 as string) as AdminYear10,
    cast(Exam_Code_10 as string) as ExamCode10,
    cast(Exam_Grade_10 as string) as ExamGrade10,
    cast(Irregularity_Code__1_10 as string) as IrregularityCode110,
    cast(Irregularity_Code__2_10 as string) as IrregularityCode210,
    cast(Admin_Year_11 as string) as AdminYear11,
    cast(Exam_Code_11 as string) as ExamCode11,
    cast(Exam_Grade_11 as string) as ExamGrade11,
    cast(Irregularity_Code__1_11 as string) as IrregularityCode111,
    cast(Irregularity_Code__2_11 as string) as IrregularityCode211,
    cast(Admin_Year_12 as string) as AdminYear12,
    cast(Exam_Code_12 as string) as ExamCode12,
    cast(Exam_Grade_12 as string) as ExamGrade12,
    cast(Irregularity_Code__1_12 as string) as IrregularityCode112,
    cast(Irregularity_Code__2_12 as string) as IrregularityCode212,
    cast(Admin_Year_13 as string) as AdminYear13,
    cast(Exam_Code_13 as string) as ExamCode13,
    cast(Exam_Grade_13 as string) as ExamGrade13,
    cast(Irregularity_Code__1_13 as string) as IrregularityCode113,
    cast(Irregularity_Code__2_13 as string) as IrregularityCode213,
    cast(Admin_Year_14 as string) as AdminYear14,
    cast(Exam_Code_14 as string) as ExamCode14,
    cast(Exam_Grade_14 as string) as ExamGrade14,
    cast(Irregularity_Code__1_14 as string) as IrregularityCode114,
    cast(Irregularity_Code__2_14 as string) as IrregularityCode214,
    cast(Admin_Year_15 as string) as AdminYear15,
    cast(Exam_Code_15 as string) as ExamCode15,
    cast(Exam_Grade_15 as string) as ExamGrade15,
    cast(Irregularity_Code__1_15 as string) as IrregularityCode115,
    cast(Irregularity_Code__2_15 as string) as IrregularityCode215,
    cast(Admin_Year_16 as string) as AdminYear16,
    cast(Exam_Code_16 as string) as ExamCode16,
    cast(Exam_Grade_16 as string) as ExamGrade16,
    cast(Irregularity_Code__1_16 as string) as IrregularityCode116,
    cast(Irregularity_Code__2_16 as string) as IrregularityCode216,
    cast(Admin_Year_17 as string) as AdminYear17,
    cast(Exam_Code_17 as string) as ExamCode17,
    cast(Exam_Grade_17 as string) as ExamGrade17,
    cast(Irregularity_Code__1_17 as string) as IrregularityCode117,
    cast(Irregularity_Code__2_17 as string) as IrregularityCode217,
    cast(Admin_Year_18 as string) as AdminYear18,
    cast(Exam_Code_18 as string) as ExamCode18,
    cast(Exam_Grade_18 as string) as ExamGrade18,
    cast(Irregularity_Code__1_18 as string) as IrregularityCode118,
    cast(Irregularity_Code__2_18 as string) as IrregularityCode218,
    cast(Admin_Year_19 as string) as AdminYear19,
    cast(Exam_Code_19 as string) as ExamCode19,
    cast(Exam_Grade_19 as string) as ExamGrade19,
    cast(Irregularity_Code__1_19 as string) as IrregularityCode119,
    cast(Irregularity_Code__2_19 as string) as IrregularityCode219,
    cast(Admin_Year_20 as string) as AdminYear20,
    cast(Exam_Code_20 as string) as ExamCode20,
    cast(Exam_Grade_20 as string) as ExamGrade20,
    cast(Irregularity_Code__1_20 as string) as IrregularityCode120,
    cast(Irregularity_Code__2_20 as string) as IrregularityCode220,
    cast(Admin_Year_21 as string) as AdminYear21,
    cast(Exam_Code_21 as string) as ExamCode21,
    cast(Exam_Grade_21 as string) as ExamGrade21,
    cast(Irregularity_Code__1_21 as string) as IrregularityCode121,
    cast(Irregularity_Code__2_21 as string) as IrregularityCode221,
    cast(Admin_Year_22 as string) as AdminYear22,
    cast(Exam_Code_22 as string) as ExamCode22,
    cast(Exam_Grade_22 as string) as ExamGrade22,
    cast(Irregularity_Code__1_22 as string) as IrregularityCode122,
    cast(Irregularity_Code__2_22 as string) as IrregularityCode222,
    cast(Admin_Year_23 as string) as AdminYear23,
    cast(Exam_Code_23 as string) as ExamCode23,
    cast(Exam_Grade_23 as string) as ExamGrade23,
    cast(Irregularity_Code__1_23 as string) as IrregularityCode123,
    cast(Irregularity_Code__2_23 as string) as IrregularityCode223,
    cast(Admin_Year_24 as string) as AdminYear24,
    cast(Exam_Code_24 as string) as ExamCode24,
    cast(Exam_Grade_24 as string) as ExamGrade24,
    cast(Irregularity_Code__1_24 as string) as IrregularityCode124,
    cast(Irregularity_Code__2_24 as string) as IrregularityCode224,
    cast(Admin_Year_25 as string) as AdminYear25,
    cast(Exam_Code_25 as string) as ExamCode25,
    cast(Exam_Grade_25 as string) as ExamGrade25,
    cast(Irregularity_Code__1_25 as string) as IrregularityCode125,
    cast(Irregularity_Code__2_25 as string) as IrregularityCode225,
    cast(Admin_Year_26 as string) as AdminYear26,
    cast(Exam_Code_26 as string) as ExamCode26,
    cast(Exam_Grade_26 as string) as ExamGrade26,
    cast(Irregularity_Code__1_26 as string) as IrregularityCode126,
    cast(Irregularity_Code__2_26 as string) as IrregularityCode226,
    cast(Admin_Year_27 as string) as AdminYear27,
    cast(Exam_Code_27 as string) as ExamCode27,
    cast(Exam_Grade_27 as string) as ExamGrade27,
    cast(Irregularity_Code__1_27 as string) as IrregularityCode127,
    cast(Irregularity_Code__2_27 as string) as IrregularityCode227,
    cast(Admin_Year_28 as string) as AdminYear28,
    cast(Exam_Code_28 as string) as ExamCode28,
    cast(Exam_Grade_28 as string) as ExamGrade28,
    cast(Irregularity_Code__1_28 as string) as IrregularityCode128,
    cast(Irregularity_Code__2_28 as string) as IrregularityCode228,
    cast(Admin_Year_29 as string) as AdminYear29,
    cast(Exam_Code_29 as string) as ExamCode29,
    cast(Exam_Grade_29 as string) as ExamGrade29,
    cast(Irregularity_Code__1_29 as string) as IrregularityCode129,
    cast(Irregularity_Code__2_29 as string) as IrregularityCode229,
    cast(Admin_Year_30 as string) as AdminYear30,
    cast(Exam_Code_30 as string) as ExamCode30,
    cast(Exam_Grade_30 as string) as ExamGrade30,
    cast(Irregularity_Code__1_30 as string) as IrregularityCode130,
    cast(Irregularity_Code__2_30 as string) as IrregularityCode230,
    cast(Student_Identifier as string) as StudentIdentifier,
    case Derived_Aggregate_Race_Ethnicity_2016_And_Forward
        when 0 then 'No Response'
        when 1 then 'American Indian/Alaska Native'
        when 2 then 'Asian'
        when 3 then 'Black/African American'
        when 4 then 'Hispanic/Latino'
        when 8 then 'Native Hawaiian or Other Pacific Islander'
        when 9 then 'White'
        when 10 then 'Other'
        when 12 then 'Two or More Races, Non-Hispanic'
    end as RaceEthnicity
from {{ source('RawData', 'ApStudentDatafile2018') }}
