-- Columns dropped: Filler
WITH
  chrabs_1617 AS (
    SELECT *
    FROM {{ source('RawData', 'CdeChrAbs1617')}}
  ),

  chrabs_1718 AS (
    SELECT *
    FROM {{ source('RawData', 'CdeChrAbs1718')}}
  ),

  chrabs_1819 AS (
    SELECT *
    FROM {{ source('RawData', 'CdeChrAbs1819')}}
  ),
  
  --Note: no 2019-20 chronic absenteeism file due to the pandemic

  chrabs_2021 AS (
    SELECT
      Academic_Year AS AcademicYear,
      Aggregate_Level AS AggregateLevel,
      County_Code AS CountyCode,
      District_Code AS DistrictCode,
      School_Code AS SchoolCode,
      County_Name AS CountyName,
      District_Name AS DistrictName,
      School_Name AS SchoolName,
      Charter_School AS CharterSchool,
      Reporting_Category AS ReportingCategory,
      ChronicAbsenteeismEligibleCumula,
      ChronicAbsenteeismCount,
      ChronicAbsenteeismRate
    FROM {{ source('RawData', 'CdeChrAbs2021')}}
  ),

  unioned AS (
    SELECT * FROM chrabs_1617
    UNION ALL
    SELECT * FROM chrabs_1718
    UNION ALL
    SELECT * FROM chrabs_1819
    UNION ALL
    SELECT * FROM chrabs_2021
  ),

  final AS (
    SELECT
      AcademicYear,
      AggregateLevel,
      CASE
        WHEN AggregateLevel = 'T' THEN 'State'
        WHEN AggregateLevel = 'C' THEN 'County'
        WHEN AggregateLevel = 'D' THEN 'District'
        WHEN AggregateLevel = 'S' THEN 'School'
      END AS EntityType,
      FORMAT("%02d", CAST(CountyCode AS INT64)) AS CountyCode,
      FORMAT("%05d", CAST(DistrictCode AS INT64)) AS DistrictCode,
      FORMAT("%07d", CAST(SchoolCode AS INT64)) AS SchoolCode,
      CountyName,
      DistrictName,
      SchoolName,
      CharterYN AS CharterSchool,
      ReportingCategory,
      CAST(ChronicAbsenteeismEligibleCumula AS INT64) AS ChronicAbsenteeismEligibleCumula,
      CAST(ChronicAbsenteeismCount AS INT64) AS ChronicAbsenteeismCount,
      ROUND(CAST(ChronicAbsenteeismRate AS FLOAT64)/100, 3) AS ChronicAbsenteeismRate
    FROM unioned
    WHERE
      AggregateLevel = 'T'
      OR (
        AggregateLevel = 'C' 
        AND CountyCode = 43
      )
      OR DistrictCode IN (
        10439, --SCCOE
        69369, --ARUSD
        69450, --FMSD
        69666, --SJUSD
        69427 -- ESUHSD
      )
  )

SELECT * FROM final