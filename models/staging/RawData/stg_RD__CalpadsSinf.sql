with sinf as (
    select
        'Empower' as SchoolName,
        *
    from {{ source('RawData', 'CalpadsSinfEmpower') }}
    union all
    select
        'Esperanza' as SchoolName,
        *
    from {{ source('RawData', 'CalpadsSinfEsperanza') }}
    union all
    select
        'Inspire' as SchoolName, 
        *
    from {{ source('RawData', 'CalpadsSinfInspire') }}
    union all
    select
        'High School' as SchoolName,
        *
    from {{ source('RawData', 'CalpadsSinfHighSchool') }}
    
),

sinf_dates as (
    select 
        substr(TableName, 12) as SchoolName,
        *
    from {{ ref('stg_GSD__ManuallyMaintainedFilesTracker')}}
    where TableName like 'CalpadsSinf%'
),

sinf_with_file_date as (
    select
        sinf_dates.DateTableLastUpdated as ExtractDate,
        sinf.*
    from sinf
    inner join sinf_dates
    on sinf.SchoolName = sinf_dates.SchoolName
)

select
    ExtractDate,
    Record_Type_Code as RecordTypeCode,
    date(concat(
        substr(Effective_Start_Date, 1, 4),
        '-',
        substr(Effective_Start_Date, 5, 2),
        '-',
        substr(Effective_Start_Date, 7, 2)
    )) as EffectiveStartDate,
    date(concat(
        substr(Effective_End_Date, 1, 4),
        '-',
        substr(Effective_End_Date, 5, 2),
        '-',
        substr(Effective_End_Date, 7, 2)
    )) as EffectiveEndDate,
    cast(cast(School_of_Attendance as int64) as string) as SchoolId,
    concat(
        substr(Academic_Year_ID, 1, 4),
        '-',
        substr(Academic_Year_ID, 8, 2)
     ) as SchoolYear,
    SSID as StateUniqueId,
    Local_Student_ID as StudentUniqueId,
    Student_Legal_First_Name as FirstName,
    Student_Legal_Last_Name as LastName,
    date(concat(
        substr(Student_Initial_US_School_Enrollment_Date_K_12, 1, 4),
        '-',
        substr(Student_Initial_US_School_Enrollment_Date_K_12, 5, 2),
        '-',
        substr(Student_Initial_US_School_Enrollment_Date_K_12, 7, 2)
    )) as InitialUsSchoolEnrollmentDateK12
from sinf_with_file_date