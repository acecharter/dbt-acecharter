with sela as (
    select
        'Empower' as SchoolName,
        *
    from {{ source('RawData', 'CalpadsSelaEmpower') }}
    union all
    select
        'Esperanza' as SchoolName,
        *
    from {{ source('RawData', 'CalpadsSelaEsperanza') }}
    union all
    select
        'Inspire' as SchoolName,
        *
    from {{ source('RawData', 'CalpadsSelaInspire') }}
    select
        'High School' as SchoolName,
        *
    from {{ source('RawData', 'CalpadsSelaHighSchool') }}
    union all
    
),

sela_dates as (
    select 
        substr(TableName, 12) as SchoolName,
        *
    from {{ ref('stg_GSD__ManuallyMaintainedFilesTracker')}}
    where TableName like 'CalpadsSela%'
),

sela_with_file_date as (
    select
        sela_dates.DateTableLastUpdated as ExtractDate,
        sela.*
    from sela
    inner join sela_dates
    on sela.SchoolName = sela_dates.SchoolName
)

select
    ExtractDate,
    Record_Type_Code as RecordTypeCode,
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
    English_Language_Acquisition_Status_Code as ElaStatusCode,
    date(concat(
        substr(English_Language_Acquisition_Status_Start_Date, 1, 4),
        '-',
        substr(English_Language_Acquisition_Status_Start_Date, 5, 2),
        '-',
        substr(English_Language_Acquisition_Status_Start_Date, 7, 2)
    )) as ElaStatusStartDate,
    Primary_Language_Code as PrimaryLanguageCode
from sela_with_file_date