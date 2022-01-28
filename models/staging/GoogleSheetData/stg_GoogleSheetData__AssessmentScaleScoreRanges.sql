  SELECT
    CAST(AceAssessmentUniqueID AS STRING) AS AceAssessmentUniqueID,
    * EXCEPT (AceAssessmentUniqueID)
  FROM {{ source('GoogleSheetData', 'AssessmentScaleScoreRanges')}}