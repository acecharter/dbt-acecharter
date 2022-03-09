WITH final AS(
  SELECT * FROM {{ ref('int_TomsElpacEnrolled2021__melted_2021') }}
  UNION ALL
  SELECT * FROM {{ ref('int_TomsElpacEnrolled2021__melted_2020') }}
  UNION ALL
  SELECT * FROM {{ ref('int_TomsElpacEnrolled2021__melted_2019') }}
)

SELECT
  AceAssessmentId,
  StateUniqueId,
  TestedSchoolId,
  SchoolYear,
  AssessmentId,
  AssessedGradeLevel,
  RecordType,
  AssessmentObjective,
  ReportingMethod,
  StudentResultDataType,
  StudentResult
FROM final