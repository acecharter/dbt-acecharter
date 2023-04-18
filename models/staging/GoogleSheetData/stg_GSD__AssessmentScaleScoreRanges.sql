SELECT
    CAST(AceAssessmentId AS STRING) AS AceAssessmentId,
    * EXCEPT (AceAssessmentId)
FROM {{ source('GoogleSheetData', 'AssessmentScaleScoreRanges')}}