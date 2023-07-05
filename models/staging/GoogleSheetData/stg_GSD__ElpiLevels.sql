select
    cast(AceAssessmentId as string) as AceAssessmentId,
    * except (AceAssessmentId)
from {{ source('GoogleSheetData', 'ElpiLevels') }}
