select
    2019 as Year,
    FORMAT("%014d", CDS_CODE) as CDS_CODE,
    * EXCEPT(CDS_CODE)
from {{ source('RawData', 'CdeEnr19')}}