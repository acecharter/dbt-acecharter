with unpivoted as (
    {{ dbt_utils.unpivot(
        relation=ref('int_CdeCgr__unioned'),
        cast_to='STRING',
        exclude=[
            'AcademicYear',
            'AggregateLevel',
            'EntityType',
            'CountyCode',
            'DistrictCode',
            'SchoolCode',
            'CountyName',
            'DistrictName',
            'SchoolName',
            'CharterSchool',
            'DASS',
            'ReportingCategory',
            'CompleterType',
            'CgrPeriodType',
            'HighSchoolCompleters',
            'EnrolledInCollegeTotal'  
        ],
        remove=[
            'CollegeGoingRateTotal',
            'EnrolledInState',
            'EnrolledOutOfState',
            'NotEnrolledInCollege'
        ],
        field_name='CollegeGoingGroupType',
        value_name='Count'
    ) }}
)

select
    AcademicYear,
    case
        when EntityType = 'State' then '00'
        when EntityType = 'County' then CountyCode
        when EntityType = 'District' then DistrictCode
        when EntityType = 'School' then SchoolCode
    end as EntityCode,
    CharterSchool,
    DASS,
    ReportingCategory,
    CompleterType,
    CgrPeriodType,
    HighSchoolCompleters,
    EnrolledInCollegeTotal,
    CollegeGoingGroupType,
    Count,
    case
        when EnrolledInCollegeTotal = 0 then null else
            round(cast(Count as int64) / EnrolledInCollegeTotal, 3)
    end as PercentOfHsCompleters
from unpivoted
