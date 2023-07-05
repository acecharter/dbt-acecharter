select
    cast(Demographic_ID as string) as DemographicId,
    Demographic_Name as DemographicName,
    Student_Group as StudentGroup
from {{ source('RawData', 'CaasppStudentGroups') }}
