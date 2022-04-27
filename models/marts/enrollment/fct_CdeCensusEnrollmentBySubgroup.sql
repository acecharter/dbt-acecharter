WITH
  schools AS (
    SELECT *
    FROM {{ ref('stg_GSD__Schools') }}
  ),

  enr AS (
    SELECT *      
    FROM {{ ref('stg_GSD__CdeEnrBySubgroup') }}
  ),

  final AS (
    SELECT
      s.StateCdsCode,
      s.StateCountyCode,
      s.StateDistrictCode,
      s.SchoolNameFull,
      s.SchoolNameMid AS SchoolName,
      s.SchoolNameShort,
      e.* EXCEPT (SchoolCode, SchoolName)
    FROM schools AS s
    LEFT JOIN enr AS e
    ON s.StateSchoolCode = e.SchoolCode
  )

SELECT * FROM final
