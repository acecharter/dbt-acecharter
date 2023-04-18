SELECT
    CAST(Test_ID AS STRING) AS TestId,
    Test_Name AS TestName
FROM {{ source('RawData', 'CaasppTests')}}