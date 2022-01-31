SELECT
    AceAssessmentId,
    StateUniqueId,
    SchoolId,
    SchoolYear,
    AssessmentID AS AssessmentId,
    CAST(AdministrationDate AS STRING) AS AdministrationDate,
    AssessedGradeLevel,
    ResultType,
    StudentResult
FROM {{ ref('int_StarMath__melted') }}

UNION ALL

SELECT
    AceAssessmentId,
    StateUniqueId,
    SchoolId,
    SchoolYear,
    AssessmentID AS AssessmentId,
    CAST(AdministrationDate AS STRING) AS AdministrationDate,
    AssessedGradeLevel,
    ResultType,
    StudentResult
FROM {{ ref('int_StarReading__melted') }}

UNION ALL

SELECT
    AceAssessmentId,
    StateUniqueId,
    SchoolId,
    SchoolYear,
    AssessmentId,
    CAST(NULL AS STRING) AS AdministrationDate,
    AssessedGradeLevel
    RecordType,
    ResultType,
    StudentResult
FROM {{ ref('int_TomsCaasppEnrolled2021__melted') }}
