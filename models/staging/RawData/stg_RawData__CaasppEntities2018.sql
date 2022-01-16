-- Columns dropped: Filler

SELECT 
  FORMAT("%02d", County_Code) AS CountyCode,
  FORMAT("%05d", District_Code) AS DistrictCode,
  FORMAT("%07d", School_Code) AS SchoolCode,
  Test_Year AS TestYear,
  CAST(Type_Id AS STRING) AS TypeId,
  County_Name AS CountyName,
  District_Name AS DistrictName,
  School_Name AS SchoolName,
  CAST(Zip_Code AS STRING) AS ZipCode

FROM {{ source('RawData', 'CaasppEntities2018')}}