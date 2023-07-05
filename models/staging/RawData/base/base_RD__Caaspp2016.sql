select
    format('%02d', County_Code) as CountyCode,
    format('%05d', District_Code) as DistrictCode,
    format('%07d', School_Code) as SchoolCode,
    Filler,
    cast(Test_Year as int64) as TestYear,
    cast(Subgroup_ID as string) as DemographicId,
    Test_Type as TestType,
    cast(Total_Tested_At_Entity_Level as int64) as TotalTestedAtReportingLevel,
    cast(Total_Tested_with_Scores as int64) as TotalTestedWithScoresAtReportingLevel,
    cast(Grade as int64) as GradeLevel,
    cast(Test_Id as string) as TestId,
    cast(CAASPP_Reported_Enrollment as int64) as StudentsEnrolled,
    cast(Students_Tested as int64) as StudentsTested,
    cast(
        case
            when Mean_Scale_Score not in ('*', '', '0.0') then Mean_Scale_Score
        end as float64
    ) as MeanScaleScore,
    cast(nullif(Percentage_Standard_Exceeded, '*') as float64) as PctStandardExceeded,
    cast(nullif(Percentage_Standard_Met, '*') as float64) as PctStandardMet,
    cast(nullif(Percentage_Standard_Met_and_Above, '*') as float64) as PctStandardMetandAbove,
    cast(nullif(Percentage_Standard_Nearly_Met, '*') as float64) as PctStandardNearlyMet,
    cast(nullif(Percentage_Standard_Not_Met, '*') as float64) as PctStandardNotMet,
    cast(Students_with_Scores as int64) as StudentsWithScores,
    cast(nullif(Area_1_Percentage_Above_Standard, '*') as float64) as Area1PctAboveStandard,
    cast(nullif(Area_1_Percentage_Near_Standard, '*') as float64) as Area1PctNearStandard,
    cast(nullif(Area_1_Percentage_Below_Standard, '*') as float64) as Area1PctBelowStandard,
    cast(nullif(Area_2_Percentage_Above_Standard, '*') as float64) as Area2PctAboveStandard,
    cast(nullif(Area_2_Percentage_Near_Standard, '*') as float64) as Area2PctNearStandard,
    cast(nullif(Area_2_Percentage_Below_Standard, '*') as float64) as Area2PctBelowStandard,
    cast(nullif(Area_3_Percentage_Above_Standard, '*') as float64) as Area3PctAboveStandard,
    cast(nullif(Area_3_Percentage_Near_Standard, '*') as float64) as Area3PctNearStandard,
    cast(nullif(Area_3_Percentage_Below_Standard, '*') as float64) as Area3PctBelowStandard,
    cast(nullif(Area_4_Percentage_Above_Standard, '*') as float64) as Area4PctAboveStandard,
    cast(nullif(Area_4_Percentage_Near_Standard, '*') as float64) as Area4PctNearStandard,
    cast(nullif(Area_4_Percentage_Below_Standard, '*') as float64) as Area4PctBelowStandard,
    cast(null as string) as TypeId
from {{ source('RawData', 'Caaspp2016') }}
