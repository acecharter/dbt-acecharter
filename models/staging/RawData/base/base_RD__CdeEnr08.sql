select
    2008 as Year,
    format('%014d', CDS_CODE) as CDS_CODE,
    * except (CDS_CODE)
from {{ source('RawData', 'CdeEnr08') }}
