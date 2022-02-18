WITH assessment_names AS (
  SELECT * FROM {{ ref('stg_GSD__Assessments') }}
),

star AS (    
  SELECT
    AceAssessmentId,
    StateUniqueId,
    TestedSchoolId,
    SchoolYear AS AssessmentSchoolYear,
    AssessmentID AS AssessmentId,
    CAST(AssessmentDate AS STRING) AS AssessmentDate,
    AssessedGradeLevel,
    ReportingMethod,
    StudentResultDataType,
    StudentResult
  FROM {{ ref('int_RenStar__unioned_melted') }}
),

caaspp_2021 AS (
  SELECT
    AceAssessmentId,
    StateUniqueId,
    TestedSchoolId,
    SchoolYear AS AssessmentSchoolYear,
    AssessmentId,
    CAST(NULL AS STRING) AS AssessmentDate,
    AssessedGradeLevel,
    ReportingMethod,
    StudentResultDataType,
    StudentResult
  FROM {{ ref('int_TomsCaasppEnrolled2021__melted_unioned') }}
),

elpac_2021 AS (
  SELECT
    AceAssessmentId,
    StateUniqueId,
    TestedSchoolId,
    SchoolYear AS AssessmentSchoolYear,
    AssessmentId,
    CAST(NULL AS STRING) AS AssessmentDate,
    AssessedGradeLevel,
    ReportingMethod,
    StudentResultDataType,
    StudentResult
FROM {{ ref('int_TomsElpacEnrolled2021__melted_unioned') }}
),

unioned_results AS (
    SELECT * FROM star
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
    r.TestedSchoolId,
    r.AssessmentSchoolYear,
    r.AssessmentId,
    r.AssessmentDate,
    r.AssessedGradeLevel,
    r.ReportingMethod,
    r.StudentResultDataType,
    r.StudentResult
  FROM unioned_results AS r
  LEFT JOIN assessment_names AS n
  USING (AceAssessmentId)
)

SELECT * FROM final