select
    '2021-22' as schoolyear,
    schoolid,
    nameofinstitution as schoolname,
    studentuniqueid,
    lastsurname as lastname,
    firstname,
    cast(gradelevel as int64) as gradelevel,
    entrydate,
    exitwithdrawdate,
    exitwithdrawreason,
    false as iscurrentenrollment
from {{ source("StarterPack_Archive", "StudentEnrollments_SY22") }}
where studentuniqueid != '16348'  -- Exclude this fake/test student showing up in PS
