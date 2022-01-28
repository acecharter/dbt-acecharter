  SELECT
    CAST(AceAssessmentUniqueID AS STRING) AS AceAssessmentUniqueID,
    AssessmentFamilyId,
    AssessmentNameLong,
    AssessmentNameShort,
    CAST(SystemOrVendorAssessmentID AS STRING) AS SystemOrVendorAssessmentID
  FROM {{ source('GoogleSheetData', 'Assessments')}}