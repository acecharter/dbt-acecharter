select
    cast(SchoolId as string) as SchoolId,
    format("%014d", StateCdsCode) as StateCdsCode,
    format("%02d", StateCountyCode) as StateCountyCode,
    format("%02d", StateDistrictCode) as StateDistrictCode,
    format("%07d", StateSchoolCode) as StateSchoolCode,
    SchoolNameFull,
    SchoolNameMid,
    SchoolNameShort,
    SchoolType,
    MinGrade,
    MaxGrade,
    concat(cast(MinGrade as string), "-", cast(MaxGrade as string))
        as GradesServed,
    Grade5,
    Grade6,
    Grade7,
    Grade8,
    Grade9,
    Grade10,
    Grade11,
    Grade12,
    cast(YearOpened as int64) as YearOpened,
    PreviousRenewalYears,
    date(CurrentCharterTermStartDate) as CurrentCharterTermStartDate,
    date(CurrentCharterTermEndDate) as CurrentCharterTermEndDate
from {{ source('GoogleSheetData', 'Schools') }}
