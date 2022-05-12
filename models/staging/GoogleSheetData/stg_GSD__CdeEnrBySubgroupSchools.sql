SELECT
  FORMAT("%07d", SchoolCode) AS SchoolCode,
  * EXCEPT(SchoolCode)
FROM {{ source('GoogleSheetData', 'CdeEnrBySubgroupSchools')}}