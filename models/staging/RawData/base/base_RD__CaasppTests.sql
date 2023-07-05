select
    cast(Test_ID as string) as TestId,
    Test_Name as TestName
from {{ source('RawData', 'CaasppTests') }}
