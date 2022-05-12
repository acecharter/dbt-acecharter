WITH
  race_and_grade AS (
    SELECT
      SchoolYear,
      EntityCode,
      SchoolType,
      'Race/Ethnicity' AS SubgroupType,
      RaceEthnicity AS Subgroup,
      Gender,
      GradeLevel,
      Enrollment,
      PctOfTotalEnrollment
    FROM {{ ref('int_CdeEnrByRaceAndGrade__3_melted')}}
  ),

  subgroups AS (
    SELECT
      SchoolYear,
      EntityCode,
      EnrollmentType AS SchoolType,
      'Other' AS SubgroupType,
      Subgroup,
      'All' AS Gender,
      'All' AS GradeLevel,
      Enrollment,
      PctOfTotalEnrollment
    FROM {{ ref('int_CdeEnrBySubgroup__transformed_unioned')}}
  ),

  final AS (
    SELECT * FROM race_and_grade
    UNION ALL
    SELECT * FROM subgroups
  )

SELECT * FROM final
