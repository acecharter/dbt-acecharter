with seis_update_dates as (
    select
        case TableName
            when 'SeisEmpower' then 'ACE Empower Academy'
            when 'SeisEsperanza' then 'ACE Esperanza Middle'
            when 'SeisInspire' then 'ACE Inspire Academy'
            when 'SeisHighSchool' then 'ACE Charter High'
        end as SchoolName,
        DateTableLastUpdated
    from {{ source('GoogleSheetData', 'ManuallyMaintainedFilesTracker') }}
),

seis_unioned as (
    select
        * except (School_CDS_Code),
        cast(School_CDS_Code as string) as School_CDS_Code
    from {{ source('RawData', 'SeisEmpower') }}
    union all
    select
        * except (School_CDS_Code),
        cast(School_CDS_Code as string) as School_CDS_Code
    from {{ source('RawData', 'SeisEsperanza') }}
    union all
    select
        * except (School_CDS_Code),
        cast(School_CDS_Code as string) as School_CDS_Code
    from {{ source('RawData', 'SeisInspire') }}
    union all
    select
        * except (School_CDS_Code),
        cast(School_CDS_Code as string) as School_CDS_Code
    from {{ source('RawData', 'SeisHighSchool') }}
),

seis as (
    select
        cast(SEIS_ID as string) as SeisUniqueId,
        cast(Student_SSID as string) as StateUniqueId,
        Last_Name as LastName,
        First_Name as FirstName,
        Date_of_Birth as BirthDate,
        case School_CDS_Code
            when '116814' then '0116814'
            when '125617' then '0125617'
            when '12924a' then '0129247'
            when '013165a' then '0131656'
        end as StateSchoolCode,
        School_of_Attendance as SchoolName,
        Grade_Code as GradeLevel,
        Student_Eligibility_Status as StudentEligibilityStatus,
        Date_of_original_SpEd_Entry as SpedEntryDate,
        Disability_1_Code as Disability1Code,
        Disability_1 as Disability1,
        Disability_2_Code as Disability2Code,
        Disability_2 as Disability2,
        Date_of_Exit_from_SpEd as SpedExitDate,
        Student_Exited as StudentExited
    from seis_unioned
)

select
    s.*,
    d.DateTableLastUpdated as SeisExtractDate
from seis as s
left join seis_update_dates as d
    on s.SchoolName = d.SchoolName
