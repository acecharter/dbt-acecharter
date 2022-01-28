WITH assessments AS (
  SELECT
    CAST(AceAssessmentUniqueId AS STRING) AS AceAssessmentUniqueId,
    AssessmentFamilyId,
    AssessmentNameLong,
    AssessmentNameShort,
    CAST(SystemOrVendorAssessmentId AS STRING) AS SystemOrVendorAssessmentId
  FROM {{ source('GoogleSheetData', 'Assessments')}}
),

assessment_families AS(
  SELECT * FROM {{ source('GoogleSheetData', 'AssessmentFamilies')}}
)

SELECT
  a.AceAssessmentUniqueId,
  a.AssessmentNameLong,
  a.AssessmentNameShort,
  a.AssessmentFamilyId,
  f.AssessmentFamilyName,
  f.SystemOrVendor AS SystemOrVendorName,
  a.SystemOrVendorAssessmentId
FROM assessments AS a
LEFT JOIN assessment_families AS f
Using (AssessmentFamilyId)