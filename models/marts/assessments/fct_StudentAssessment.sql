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

UNION ALL
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

UNION ALL
SELECT
    AceAssessmentId,
    StateUniqueId,
    SchoolId,
    SchoolYear AS AssessmentSchoolYear,
    AssessmentId,
    CAST(NULL AS STRING) AS AdministrationDate,
    AssessedGradeLevel
    RecordType,
    ReportingMethod,
    StudentResultDataType,
    StudentResult
FROM {{ ref('int_TomsCaasppEnrolled2021__melted') }}

UNION ALL
SELECT
    AceAssessmentId,
    StateUniqueId,
    SchoolId,
    SchoolYear AS AssessmentSchoolYear,
    AssessmentId,
    CAST(NULL AS STRING) AS AdministrationDate,
    AssessedGradeLevel
    RecordType,
    ReportingMethod,
    StudentResultDataType,
    StudentResult
FROM {{ ref('int_TomsElpacEnrolled2021__melted') }}
