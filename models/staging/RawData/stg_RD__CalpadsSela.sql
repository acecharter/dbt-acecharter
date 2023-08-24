with sela as (
    select * from {{ source('RawData', 'CalpadsSelaEmpower') }}
    union all select * from {{ source('RawData', 'CalpadsSelaEsperanza') }}
    union all select * from {{ source('RawData', 'CalpadsSelaInspire') }}
    
) 

select
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
from sela