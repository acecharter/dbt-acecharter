{{ config(
    materialized='table'
)}}

WITH 
  enr08 AS (
    SELECT
      2008 AS Year,
      *
    FROM {{ source('RawData', 'CdeEnr08')}}
  ), 

  enr09 AS (
    SELECT
      2009 AS Year,
      *
    FROM {{ source('RawData', 'CdeEnr09')}}
  ), 

  enr10 AS (
    SELECT
      2010 AS Year,
      *
    FROM {{ source('RawData', 'CdeEnr10')}}
  ), 

  enr11 AS (
    SELECT
      2011 AS Year,
      *
    FROM {{ source('RawData', 'CdeEnr11')}}
  ),
   

  enr12 AS (
    SELECT
      2012 AS Year,
      *
    FROM {{ source('RawData', 'CdeEnr12')}}
  ),
   

  enr13 AS (
    SELECT
      2013 AS Year,
      *
    FROM {{ source('RawData', 'CdeEnr13')}}
  ),
   

  enr14 AS (
    SELECT
      2014 AS Year,
      *
    FROM {{ source('RawData', 'CdeEnr14')}}
  ),
   

  enr15 AS (
    SELECT
      2015 AS Year,
      *
    FROM {{ source('RawData', 'CdeEnr15')}}
  ),
   

  enr16 AS (
    SELECT
      2016 AS Year,
      *
    FROM {{ source('RawData', 'CdeEnr16')}}
  ),
   

  enr17 AS (
    SELECT
      2017 AS Year,
      *
    FROM {{ source('RawData', 'CdeEnr17')}}
  ),
   

  enr18 AS (
    SELECT
      2018 AS Year,
      *
    FROM {{ source('RawData', 'CdeEnr18')}}
  ),
   

  enr19 AS (
    SELECT
      2019 AS Year,
      *
    FROM {{ source('RawData', 'CdeEnr19')}}
  ),
   

  enr20 AS (
    SELECT
      2020 AS Year,
      *
    FROM {{ source('RawData', 'CdeEnr20')}}
  ),
   

  enr21 AS (
    SELECT
      2021 AS Year,
      *
    FROM {{ source('RawData', 'CdeEnr21')}}
  ),

  unioned AS (
    SELECT * FROM enr08
    UNION ALL
    SELECT * FROM enr09
    UNION ALL
    SELECT * FROM enr10
    UNION ALL
    SELECT * FROM enr11
    UNION ALL
    SELECT * FROM enr12
    UNION ALL
    SELECT * FROM enr13
    UNION ALL
    SELECT * FROM enr14
    UNION ALL
    SELECT * FROM enr15
    UNION ALL
    SELECT * FROM enr16
    UNION ALL
    SELECT * FROM enr17
    UNION ALL
    SELECT * FROM enr18
    UNION ALL
    SELECT * FROM enr19
    UNION ALL
    SELECT * FROM enr20
    UNION ALL
    SELECT * FROM enr21
  ),

  final AS (
    SELECT
      Year,
      FORMAT("%014d", CDS_CODE) AS CdsCode,
      COUNTY AS County,
      DISTRICT AS District,
      SCHOOL AS School,
      ETHNIC AS EthnicCode,
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
    FROM unioned
  )


SELECT * FROM final
