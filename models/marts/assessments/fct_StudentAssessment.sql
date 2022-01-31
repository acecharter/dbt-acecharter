WITH assessment_names AS (
  SELECT * FROM {{ ref('stg_GoogleSheetData__Assessments') }}
),

star_math AS (    
  SELECT
    AceAssessmentId,
    StateUniqueId,
    SchoolId,
    SchoolYear AS AssessmentSchoolYear,
    AssessmentID AS AssessmentId,
    CAST(AdministrationDate AS STRING) AS AdministrationDate,
    AssessedGradeLevel,
    ReportingMethod,
    StudentResultDataType,
    StudentResult
  FROM {{ ref('int_StarMath__melted') }}
),

star_reading AS (
  SELECT
    AceAssessmentId,
    StateUniqueId,
    SchoolId,
    SchoolYear AS AssessmentSchoolYear,
    AssessmentID AS AssessmentId,
    CAST(AdministrationDate AS STRING) AS AdministrationDate,
    AssessedGradeLevel,
    ReportingMethod,
    StudentResultDataType,
    StudentResult
  FROM {{ ref('int_StarReading__melted') }}
),

caaspp_2021 AS (
  SELECT
    AceAssessmentId,
    StateUniqueId,
    SchoolId,
    SchoolYear AS AssessmentSchoolYear,
    AssessmentId,
    CAST(NULL AS STRING) AS AdministrationDate,
    AssessedGradeLevel,
    ReportingMethod,
    StudentResultDataType,
    StudentResult
  FROM {{ ref('int_TomsCaasppEnrolled2021__melted') }}
),

elpac_2021 AS (
  SELECT
    AceAssessmentId,
    StateUniqueId,
    SchoolId,
    SchoolYear AS AssessmentSchoolYear,
    AssessmentId,
    CAST(NULL AS STRING) AS AdministrationDate,
    AssessedGradeLevel,
    ReportingMethod,
    StudentResultDataType,
    StudentResult
FROM {{ ref('int_TomsElpacEnrolled2021__melted') }}
),

unioned_results AS (
    SELECT * FROM star_math
    UNION ALL 
    SELECT * FROM star_reading
    UNION ALL
    SELECT * FROM caaspp_2021
    UNION ALL
    SELECT * FROM elpac_2021

),

final AS (
  SELECT
    n.AceAssessmentId,
    n.AssessmentNameShort AS AssessmentName,
    r.StateUniqueId,
    r.SchoolId,
    r.AssessmentSchoolYear,
    r.AssessmentId,
    r.AdministrationDate,
    r.AssessedGradeLevel,
    r.ReportingMethod,
    r.StudentResultDataType,
    r.StudentResult
  FROM unioned_results AS r
  LEFT JOIN assessment_names AS n
  USING (AceAssessmentId)
)

SELECT * FROM final