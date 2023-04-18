{{ config(
    materialized='table'
)}}

WITH
    enrollment AS (
        SELECT * FROM {{ ref('base_RD__CdeEnr22')}}
        UNION ALL
        SELECT * FROM {{ ref('base_RD__CdeEnr21')}}
        UNION ALL
        SELECT * FROM {{ ref('base_RD__CdeEnr20')}}
        UNION ALL
        SELECT * FROM {{ ref('base_RD__CdeEnr19')}}
        UNION ALL
        SELECT * FROM {{ ref('base_RD__CdeEnr18')}}
        UNION ALL
        SELECT * FROM {{ ref('base_RD__CdeEnr17')}}
        UNION ALL
        SELECT * FROM {{ ref('base_RD__CdeEnr16')}}
        UNION ALL
        SELECT * FROM {{ ref('base_RD__CdeEnr15')}}
        UNION ALL
        SELECT * FROM {{ ref('base_RD__CdeEnr14')}}
        UNION ALL
        SELECT * FROM {{ ref('base_RD__CdeEnr13')}}
        UNION ALL
        SELECT * FROM {{ ref('base_RD__CdeEnr12')}}
        UNION ALL
        SELECT * FROM {{ ref('base_RD__CdeEnr11')}}
        UNION ALL
        SELECT * FROM {{ ref('base_RD__CdeEnr10')}}
        UNION ALL
        SELECT * FROM {{ ref('base_RD__CdeEnr09')}}
        UNION ALL
        SELECT * FROM {{ ref('base_RD__CdeEnr08')}}
    ),

    final AS (
        SELECT
            Year,
            CDS_CODE AS CdsCode,
            LEFT(CAST(CDS_CODE AS STRING), 2) AS CountyCode,
            SUBSTR(CAST(CDS_CODE AS STRING), 3, 5) AS DistrictCode,
            RIGHT(CAST(CDS_CODE AS STRING), 7) AS SchoolCode,
            COUNTY AS County,
            DISTRICT AS District,
            SCHOOL AS School,
            CAST(ETHNIC AS STRING) AS RaceEthnicCode,
            GENDER AS Gender,
            * EXCEPT (
                    Year,
                    CDS_CODE,
                    COUNTY,
                    DISTRICT,
                    SCHOOL,
                    ETHNIC,
                    GENDER
            )
        FROM enrollment
    )


SELECT * FROM final
