SELECT * FROM {{ ref('base_RD__Caaspp2015')}}
UNION ALL
SELECT * FROM {{ ref('base_RD__Caaspp2016')}}
UNION ALL
SELECT * FROM {{ ref('base_RD__Caaspp2017')}}
UNION ALL
SELECT * FROM {{ ref('base_RD__Caaspp2018')}}
UNION ALL
SELECT * FROM {{ ref('base_RD__Caaspp2019')}}
UNION ALL
SELECT * FROM {{ ref('base_RD__Caaspp2021')}}