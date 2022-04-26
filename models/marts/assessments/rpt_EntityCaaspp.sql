WITH 
  ace_schools AS (
    SELECT
      StateSchoolCode AS EntityCode,
      'School' AS EntityType,
      SchoolName AS EntityName,
      SchoolNameMid AS EntityNameShort
    FROM {{ ref('dim_Schools')}}
  ),

  comparison_entities AS (
    SELECT
      EntityCode,
      EntityType,
      EntityName,
      EntityNameShort
    FROM {{ ref('stg_GSD__ComparisonEntities')}}
  ),

  entities AS (
    SELECT * FROM ace_schools
    UNION ALL
    SELECT * FROM comparison_entities
  ),

  caaspp AS (
    SELECT
      EntityCode,
      TestYear,
      DemographicId,
      StudentGroup,
      DemographicName,
      GradeLevel,
      TestId,
      TestSubject,
      StudentsEnrolled,
      StudentsWithScores,
      ReportingMethod,
      ResultDataType,
      SchoolResult,
      StudentWithResultCount
    FROM {{ ref('fct_EntityCaaspp')}} 
  ),

  final AS (
    SELECT
      e.*,
      c.* EXCEPT (EntityCode)
    FROM entities AS e
    LEFT JOIN caaspp AS c
    USING (EntityCode)
  )

SELECT * FROM final
