SELECT
    u._id AS UserId,
    u.name AS UserName,
    u.email AS Email,
    DATE(u.created) AS DateCreated,
    i.school AS SchoolId,
    i.gradelevel AS GradeLevelId,
    i.course AS CourseId,
    DATE(u.lastActivity) AS DateLastActive,
    DATE(u.lastModified) AS DateLastModified,
FROM {{ source('Whetstone', 'Users_raw')}} AS u
LEFT JOIN UNNEST(defaultInformation) AS i