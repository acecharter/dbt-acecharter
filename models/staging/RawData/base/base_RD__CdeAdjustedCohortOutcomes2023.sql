with original as (
    select
        AcademicYear,
        AggregateLevel,
        CountyCode,
        DistrictCode,
        SchoolCode,
        CountyName,
        DistrictName,
        SchoolName,
        CharterSchool,
        DASS,
        ReportingCategory,
        cast(nullif(CohortStudents, '*') as int64) as CohortStudents,
        cast(nullif(Regular_HS_Diploma_Graduates__Count_, '*') as int64) as Regular_HS_Diploma_Graduates__Count_,
        cast(nullif(Regular_HS_Diploma_Graduates__Rate_, '*') as float64) as Regular_HS_Diploma_Graduates__Rate_,
        cast(nullif(Met_UC_CSU_Grad_Req_s__Count_, '*') as int64) as Met_UC_CSU_Grad_Req_s__Count_,
        cast(nullif(Met_UC_CSU_Grad_Req_s__Rate_, '*') as float64) as Met_UC_CSU_Grad_Req_s__Rate_,
        cast(nullif(Seal_of_Biliteracy__Count_, '*') as int64) as Seal_of_Biliteracy__Count_,
        cast(nullif(Seal_of_Biliteracy__Rate_, '*') as float64) as Seal_of_Biliteracy__Rate_,
        cast(nullif(Golden_State_Seal_Merit_Diploma__Count_, '*') as int64) as Golden_State_Seal_Merit_Diploma__Count_,
        cast(nullif(Golden_State_Seal_Merit_Diploma__Rate, '*') as float64) as Golden_State_Seal_Merit_Diploma__Rate,
        cast(nullif(CHSPE_Completer__Count_, '*') as int64) as CHSPE_Completer__Count_,
        cast(nullif(CHSPE_Completer__Rate_, '*') as float64) as CHSPE_Completer__Rate_,
        cast(nullif(Adult_Ed__HS_Diploma__Count_, '*') as int64) as Adult_Ed__HS_Diploma__Count_,
        cast(nullif(Adult_Ed__HS_Diploma__Rate_, '*') as float64) as Adult_Ed__HS_Diploma__Rate_,
        cast(nullif(SPED_Certificate__Count_, '*') as int64) as SPED_Certificate__Count_,
        cast(nullif(SPED_Certificate__Rate_, '*') as float64) as SPED_Certificate__Rate_,
        cast(nullif(GED_Completer__Count_, '*') as int64) as GED_Completer__Count_,
        cast(nullif(GED_Completer__Rate_, '*') as float64) as GED_Completer__Rate_,
        cast(nullif(Other_Transfer__Count_, '*') as int64) as Other_Transfer__Count_,
        cast(nullif(Other_Transfer__Rate_, '*') as float64) as Other_Transfer__Rate_,
        cast(nullif(Dropout__Count_, '*') as int64) as Dropout__Count_,
        cast(nullif(Dropout__Rate_, '*') as float64) as Dropout__Rate_,
        cast(nullif(Still_Enrolled__Count_, '*') as int64) as Still_Enrolled__Count_,
        cast(nullif(Still_Enrolled__Rate_, '*') as float64) as Still_Enrolled__Rate_
    from {{ source('RawData', 'CdeAdjustedCohortOutcomes2023')}}
),

/*Unlike previous years, in 2022-23 the CDE did not including rows where CharterSchool='All' for schools (i.e. AggregateLevel='S'). 
In order to easily filter/compare charter and non-charter results across different entity types, school results with CharterSchool='All'
are added here */
schools_CharterSchool_All_added as (
    select
        AcademicYear,
        AggregateLevel,
        CountyCode,
        DistrictCode,
        SchoolCode,
        CountyName,
        DistrictName,
        SchoolName,
        'All' as CharterSchool,
        DASS,
        ReportingCategory,
        CohortStudents,
        Regular_HS_Diploma_Graduates__Count_,
        Regular_HS_Diploma_Graduates__Rate_,
        Met_UC_CSU_Grad_Req_s__Count_,
        Met_UC_CSU_Grad_Req_s__Rate_,
        Seal_of_Biliteracy__Count_,
        Seal_of_Biliteracy__Rate_,
        Golden_State_Seal_Merit_Diploma__Count_,
        Golden_State_Seal_Merit_Diploma__Rate,
        CHSPE_Completer__Count_,
        CHSPE_Completer__Rate_,
        Adult_Ed__HS_Diploma__Count_,
        Adult_Ed__HS_Diploma__Rate_,
        SPED_Certificate__Count_,
        SPED_Certificate__Rate_,
        GED_Completer__Count_,
        GED_Completer__Rate_,
        Other_Transfer__Count_,
        Other_Transfer__Rate_,
        Dropout__Count_,
        Dropout__Rate_,
        Still_Enrolled__Count_,
        Still_Enrolled__Rate_
    from original
    where AggregateLevel = 'S'
),

final as (
    select * from original
    union all
    select * from schools_CharterSchool_All_added
)

select * from final
