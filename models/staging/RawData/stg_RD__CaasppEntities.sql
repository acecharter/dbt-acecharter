WITH unioned AS (
    SELECT * FROM {{ source('RawData', 'CaasppEntities2015')}}
    UNION ALL
    SELECT * FROM {{ source('RawData', 'CaasppEntities2016')}}
    UNION ALL
    SELECT * FROM {{ source('RawData', 'CaasppEntities2017')}}
    UNION ALL
    SELECT * FROM {{ source('RawData', 'CaasppEntities2018')}}
    UNION ALL
    SELECT * FROM {{ source('RawData', 'CaasppEntities2019')}}
    UNION ALL
    SELECT
        * EXCEPT (Zip_Code),
        CAST(Zip_Code AS STRING) AS Zip_Code
    FROM {{ source('RawData', 'CaasppEntities2021')}}
),

formatted AS (
    SELECT 
        FORMAT("%02d", County_Code) AS CountyCode,
        FORMAT("%05d", District_Code) AS DistrictCode,
        FORMAT("%07d", School_Code) AS SchoolCode,
        Test_Year AS TestYear,
        CAST(Type_Id AS STRING) AS TypeId,
        County_Name AS CountyName,
        District_Name AS DistrictName,
        School_Name AS SchoolName,
        CASE
            WHEN REPLACE(Zip_Code, ' ', '') = '' THEN NULL
            ELSE CAST(Zip_Code AS STRING)
        END AS ZipCode
    FROM unioned
),

final AS (
    SELECT
        CASE
            WHEN DistrictCode = '00000' THEN CountyCode
            WHEN SchoolCode = '0000000' THEN DistrictCode
            ELSE SchoolCode
        END AS EntityCode,
        CASE
            WHEN CountyCode = '00' THEN 'State'
            WHEN CountyCode = '43' AND DistrictCode = '00000' THEN 'County'
            WHEN SchoolCode = '0000000' THEN 'District'
            ELSE 'School'
        END AS EntityType,
        *
    FROM formatted
    
)

SELECT * FROM final
