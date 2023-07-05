select
    cast(AceAssessmentId as string) as AceAssessmentId,
    AssessmentNameShort,
    AssessmentName,
    AssessmentSubject,
    AssessmentFamilyNameShort,
    AssessmentFamilyName,
    SystemOrVendorName,
    cast(SystemOrVendorAssessmentId as string) as SystemOrVendorAssessmentId
from {{ source('GoogleSheetData', 'Assessments') }}
