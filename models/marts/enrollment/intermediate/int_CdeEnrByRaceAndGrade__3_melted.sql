WITH
  enr AS (
    SELECT
      CONCAT(
        CAST(Year AS STRING),
        EntityCode,
        RaceEthnicCode,
        Gender,
        Subgroup,
        SchoolType
      ) AS UniqueEnrId,
      *      
    FROM {{ ref('int_CdeEnrByRaceAndGrade__2_filtered') }}
  ),

  enr_keys AS(
    SELECT
      UniqueEnrId,
      Year,
      CONCAT(
        CAST(Year AS STRING), 
        '-', 
        FORMAT("%02d", Year - 1999)
      ) AS SchoolYear,
      EntityCode,
      EntityType,
      EntityName,
      SchoolType,
      RaceEthnicCode,
      CASE
        WHEN RaceEthnicCode = '0' THEN 'Not Reported'
        WHEN RaceEthnicCode = '1' THEN 'American Indian or Alaska Native'
        WHEN RaceEthnicCode = '2' THEN 'Asian'
        WHEN RaceEthnicCode = '3' THEN 'Pacific Islander'
        WHEN RaceEthnicCode = '4' THEN 'Filipino'
        WHEN RaceEthnicCode = '5' THEN 'Hispanic or Latino'
        WHEN RaceEthnicCode = '6' THEN 'African American'
        WHEN RaceEthnicCode = '7' THEN 'White'
        WHEN RaceEthnicCode = '8' THEN 'Multiple or No Response (pre-2009)'
        WHEN RaceEthnicCode = '9' THEN 'Two or More Races'
      END AS RaceEthnicity, 
      Gender
    FROM enr
  ),

  kinder AS (
    SELECT
      UniqueEnrId,
      'K' AS GradeLevel,
      KDGN AS Enrollment
    FROM enr
  ),

  gr1 AS (
    SELECT
      UniqueEnrId,
      '1' AS GradeLevel,
      GR_1 AS Enrollment
    FROM enr
  ),

  gr2 AS (
    SELECT
      UniqueEnrId,
      '2' AS GradeLevel,
      GR_2 AS Enrollment
    FROM enr
  ),

  gr3 AS (
    SELECT
      UniqueEnrId,
      '3' AS GradeLevel,
      GR_3 AS Enrollment
    FROM enr
  ),
  
  gr4 AS (
    SELECT
      UniqueEnrId,
      '4' AS GradeLevel,
      GR_4 AS Enrollment
    FROM enr
  ),
  
  gr5 AS (
    SELECT
      UniqueEnrId,
      '5' AS GradeLevel,
      GR_5 AS Enrollment
    FROM enr
  ),
  
  gr6 AS (
    SELECT
      UniqueEnrId,
      '6' AS GradeLevel,
      GR_6 AS Enrollment
    FROM enr
  ),
  
  gr7 AS (
    SELECT
      UniqueEnrId,
      '7' AS GradeLevel,
      GR_7 AS Enrollment
    FROM enr
  ),
  
  gr8 AS (
    SELECT
      UniqueEnrId,
      '8' AS GradeLevel,
      GR_8 AS Enrollment
    FROM enr
  ),
  
  gr9 AS (
    SELECT
      UniqueEnrId,
      '9' AS GradeLevel,
      GR_9 AS Enrollment
    FROM enr
  ),
  
  gr10 AS (
    SELECT
      UniqueEnrId,
      '10' AS GradeLevel,
      GR_10 AS Enrollment
    FROM enr
  ),
  
  gr11 AS (
    SELECT
      UniqueEnrId,
      '11' AS GradeLevel,
      GR_11 AS Enrollment
    FROM enr
  ),
  
  gr12 AS (
    SELECT
      UniqueEnrId,
      '12' AS GradeLevel,
      GR_12 AS Enrollment
    FROM enr
  ),

  ungr_elm AS (
    SELECT
      UniqueEnrId,
      'Ungraded Elementary' AS GradeLevel,
      UNGR_ELM AS Enrollment
    FROM enr
  ),

  ungr_sec AS (
    SELECT
      UniqueEnrId,
      'Ungraded Secondary' AS GradeLevel,
      UNGR_SEC AS Enrollment
    FROM enr
  ),

  adult AS (
    SELECT
      UniqueEnrId,
      'Adult' AS GradeLevel,
      ADULT AS Enrollment
    FROM enr
  ),

  total AS (
    SELECT
      Year,
      EntityCode,
      SchoolType,
      SUM(ENR_TOTAL) AS Enrollment
    FROM enr
    WHERE Subgroup = 'All Students'
    GROUP BY 1, 2, 3
  ),

  enr_unioned AS (
    SELECT * FROM kinder
    UNION ALL
    SELECT * FROM gr1
    UNION ALL
    SELECT * FROM gr2
    UNION ALL
    SELECT * FROM gr3
    UNION ALL
    SELECT * FROM gr4
    UNION ALL
    SELECT * FROM gr5
    UNION ALL
    SELECT * FROM gr6
    UNION ALL
    SELECT * FROM gr7
    UNION ALL
    SELECT * FROM gr8
    UNION ALL
    SELECT * FROM gr9
    UNION ALL
    SELECT * FROM gr10
    UNION ALL
    SELECT * FROM gr11
    UNION ALL
    SELECT * FROM gr12
    UNION ALL
    SELECT * FROM ungr_elm
    UNION ALL
    SELECT * FROM ungr_sec
    UNION ALL
    SELECT * FROM adult
  ),

  enr_final AS (
    SELECT
      k.* EXCEPT (UniqueEnrId),
      e.GradeLevel,
      e.Enrollment
    FROM enr_keys AS k
    LEFT JOIN enr_unioned AS e
    USING (UniqueEnrId)
    WHERE e.Enrollment > 0
    ORDER BY
      SchoolYear DESC,
      EntityCode,
      RaceEthnicCode,
      Gender,
      GradeLevel
  ),

  final AS (
    SELECT
      e.*,
      ROUND(e.Enrollment / t.Enrollment, 4) AS PctOfTotalEnrollment
    FROM enr_final AS e
    LEFT JOIN total AS t
    ON
      e.Year = t.Year
      AND e.EntityCode = t.EntityCode
      AND e.SchoolType = t.SchoolType
  )

SELECT * 
FROM final
ORDER BY
  Year, 
  EntityCode, 
  RaceEthnicCode, 
  Gender, 
  GradeLevel