WITH
  entity_enr AS (
    SELECT *      
    FROM {{ ref('stg_GSD__CdeEnrBySubgroupEntities') }}
  ),

  entity_enr_charter AS (
    SELECT
      SchoolYear,
      EntityCode,
      Subgroup,
      'Charter Schools' AS EnrollmentType,
      CharterSchoolEnrollment AS Enrollment
    FROM entity_enr
  ),

  entity_enr_non_charter AS (
    SELECT
      SchoolYear,
      EntityCode,
      Subgroup,
      'Non-Charter Schools' AS EnrollmentType,
      NonCharterSchoolEnrollment AS Enrollment
    FROM entity_enr
  ),
  
  entity_enr_total AS (
    SELECT
      SchoolYear,
      EntityCode,
      Subgroup,
      'All Schools' AS EnrollmentType,
      TotalEnrollment AS Enrollment
    FROM entity_enr
  ),

  entity_enr_unioned AS (
    SELECT * FROM entity_enr_charter
    UNION ALL
    SELECT * FROM entity_enr_non_charter
    UNION ALL
    SELECT * FROM entity_enr_total
  ),

  entity_enr_all_students AS (
    SELECT * 
    FROM entity_enr_unioned
    WHERE Subgroup = 'All Students'
  ),

  entity_enr_final AS (
    SELECT
      u.*,
      ROUND(u.Enrollment / a.Enrollment, 4) AS PctOfTotalEnrollment
    FROM entity_enr_unioned AS u
    LEFT JOIN entity_enr_all_students AS a
    ON
      u.SchoolYear = a.SchoolYear
      AND u.EntityCode = a.EntityCode
      AND u.EnrollmentType = a.EnrollmentType
  ),

  school_enr AS (
    SELECT * FROM {{ ref('stg_GSD__CdeEnrBySubgroupSchools')}}
  ),

  school_enr_all_students AS (
    SELECT * 
    FROM school_enr
    WHERE Subgroup = 'All Students'
  ),

  school_enr_final AS (
    SELECT
      s.SchoolYear,
      s.SchoolCode AS EntityCode,
      s.Subgroup,
      'All Schools' AS EnrollmentType,
      s.Enrollment,
      ROUND(s.Enrollment / a.Enrollment, 4) AS PctOfTotalEnrollment
    FROM school_enr AS s
    LEFT JOIN school_enr_all_students AS a
    ON
      s.SchoolYear = a.SchoolYear
      AND s.SchoolCode = a.SchoolCode
  ),

  final AS (
    SELECT * FROM entity_enr_final
    UNION ALL
    SELECT * FROM school_enr_final
  )

SELECT * FROM final
