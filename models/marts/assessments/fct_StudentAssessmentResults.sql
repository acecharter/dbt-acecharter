SELECT
    StateUniqueId,
    SchoolId,
    SchoolYear,
    AssessmentID AS AssessmentId,
    CAST(AdministrationDate AS STRING) AS AdministrationDate,
    AssessedGradeLevel,
    ReportingMethod,
    StudentResultDataType,
    StudentResult
FROM {{ ref('int_StarMath__melted') }}

UNION ALL

SELECT
    StateUniqueId,
    SchoolId,
    SchoolYear,
    AssessmentID AS AssessmentId,
    CAST(AdministrationDate AS STRING) AS AdministrationDate,
    AssessedGradeLevel,
    ReportingMethod,
    StudentResultDataType,
    StudentResult
FROM {{ ref('int_StarReading__melted') }}

UNION ALL

SELECT
    StateUniqueId,
    SchoolId,
    SchoolYear,
    AssessmentId,
    CAST(NULL AS STRING) AS AdministrationDate,
    AssessedGradeLevel
    RecordType,
    ReportingMethod,
    StudentResultDataType,
    StudentResult
FROM {{ ref('int_TomsCaasppEnrolled2021__melted') }}