with grade_placement_2021_dates as (
    select
        'Star Reading Enterprise Tests' as Activity_Type,
        * except (DayRangeEnd),
        case
            when DayRangeEnd = 31
                then
                    case
                        when MonthNumber = 2 then 28
                        when MonthNumber in (4, 6, 9, 11) then 30
                        else DayRangeEnd
                    end
            else DayRangeEnd
        end as DayRangeEnd
    from {{ ref('stg_GSD__RenStarGradePlacementByDayRange') }}
),

grade_placement as (
    select
        *,
        case
            when
                MonthNumber >= 8
                then
                    concat(
                        '2020-',
                        format('%02d', MonthNumber),
                        '-',
                        format('%02d', DayRangeStart)
                    )
            else
                concat(
                    '2021-',
                    format('%02d', MonthNumber),
                    '-',
                    format('%02d', DayRangeStart)
                )
        end as StartDate,
        case
            when
                MonthNumber >= 8
                then
                    concat(
                        '2020-',
                        format('%02d', MonthNumber),
                        '-',
                        format('%02d', DayRangeEnd)
                    )
            else
                concat(
                    '2021-',
                    format('%02d', MonthNumber),
                    '-',
                    format('%02d', DayRangeEnd)
                )
        end as EndDate
    from grade_placement_2021_dates
),

star_reading_with_gp_added as (
    select
        s.*,
        cast(
            concat(
                s.Current_Grade, '.', gp.GradePlacementDecimalValue
            ) as float64
        ) as GradePlacement
    from {{ source('RenaissanceStar_Archive', 'Reading_SY21') }} as s
    left join grade_placement as gp
        on s.Activity_Type = gp.Activity_Type
where
    date(s.Activity_Completed_Date) between date(StartDate) and date(EndDate)
),

final as (
select
    case School_Id
        when 'gs_4e804ecc-4623-46b4-a91a-fe2acb88cbb3' then '116814'
        when 'gs_e8341d4c-4366-43e1-99b5-71f66cec337a' then '129247'
        when 'gs_b3f0fcec-7391-479a-93fc-132cf73faa43' then '131656'
        when 'gs_4cfd50a2-23e1-4e6f-bb7d-e25ddee340a2' then '125617'
        else '999999999'
    end as TestedSchoolId,
    School_Name as TestedSchoolName,
    concat(left(School_Year, 4), '-', right(School_Year, 2)) as SchoolYear,
    cast(null as string) as StudentRenaissanceID,
    cast(null as string) as StudentIdentifier,
    cast(Student_State_ID as string) as StateUniqueId,
    case
        when
            Student_Middle_Name is null
            then concat(Student_Last_Name, ', ', Student_First_Name)
        else
            concat(
                Student_Last_Name,
                ', ',
                Student_First_Name,
                ' ',
                Student_Middle_Name
            )
    end as DisplayName,
    Student_Last_Name as LastName,
    Student_First_Name as FirstName,
    Student_Middle_Name as MiddleName,
    cast(null as string) as Gender,
    date(Birthdate) as BirthDate,
    cast(Current_Grade as string) as GradeLevel,
    cast(null as string) as EnrollmentStatus,
    Renaissance_Activity_ID as AssessmentId,
    date(Activity_Completed_Date) as AssessmentDate,
    cast(null as int64) as AssessmentNumber,
    Activity_Type as AssessmentType,
    null as TotalTimeInSeconds,
    GradePlacement,
    cast(Current_Grade as string) as AssessmentGradeLevel,
    cast(Grade_Equivalent as string) as GradeEquivalent,
    Scaled_Score as ScaledScore,
    Unified_Scale as UnifiedScore,
    Percentile_Rank as PercentileRank,
    Normal_Curve_Equivalent as NormalCurveEquivalent,
    cast(Instructional_Reading_Level as string)
        as InstructionalReadingLevel,
    Lexile_Score as Lexile,
    case
        when Current_SGP_Vector = 'FALL_FALL' then Current_SGP
    end as StudentGrowthPercentileFallFall,
    case
        when Current_SGP_Vector = 'FALL_SPRING' then Current_SGP
    end as StudentGrowthPercentileFallSpring,
    case
        when Current_SGP_Vector = 'FALL_WINTER' then Current_SGP
    end as StudentGrowthPercentileFallWinter,
    case
        when Current_SGP_Vector = 'SPRING_SPRING' then Current_SGP
    end as StudentGrowthPercentileSpringSpring,
    case
        when Current_SGP_Vector = 'WINTER_SPRING' then Current_SGP
    end as StudentGrowthPercentileWinterSpring,
    Current_SGP as CurrentSGP,
    cast(right(State_Benchmark_Category, 1) as int64)
        as StateBenchmarkCategoryLevel
from star_reading_with_gp_added
)

select * from final
