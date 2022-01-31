SELECT
  CAST(AceAssessmentId AS STRING) AS AceAssessmentId,
  AssessmentNameShort,
  AssessmentName,
  AssessmentFamilyNameShort,
  AssessmentFamilyName,
  SystemOrVendorName,
  CAST(SystemOrVendorAssessmentId AS STRING) AS SystemOrVendorAssessmentId
FROM {{ source('GoogleSheetData', 'Assessments')}}