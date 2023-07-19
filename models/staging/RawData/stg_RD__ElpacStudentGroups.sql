select
    cast(Student_Group_ID as string) as StudentGroupId,
    Student_Group_Name as StudentGroupName
from {{ source('RawData', 'ElpacStudentGroups') }}
