SELECT
    '2021-22' AS SchoolYear,
    *
FROM {{ source('StarterPack_Archive', 'CourseEnrollments_v2_SY22')}}