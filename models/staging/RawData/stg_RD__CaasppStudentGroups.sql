SELECT
    CAST(Demographic_ID AS STRING) AS DemographicId,
    Demographic_Name AS DemographicName,
    Student_Group AS StudentGroup
FROM {{ source('RawData', 'CaasppStudentGroups')}}