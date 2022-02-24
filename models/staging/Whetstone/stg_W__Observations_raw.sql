SELECT
  o._id AS ObservationId,
  o.isPrivate AS IsPrivate,
  o.isPublished AS IsPublished,
  o.locked AS Locked,
  o.requireSignature AS RequireSignature,
  DATE(o.observedAt) AS DateObserved,
  ob._id AS ObserverId,
  ob.email AS ObserverEmail,
  ob.name AS ObserverName,
  t._id AS TeacherId,
  t.name AS TeacherName,
  t.email AS TeacherEmail,
  ta._id AS TeachingAssignmentId,
  ta.school AS TeachingAssignmentSchool,
  r._id AS RubricId,
  r.name AS RubricName,
  DATE(o.created) AS DateCreated,
  DATE(o.firstPublished) AS DateFirstPublished,
  DATE(o.lastPublished) AS DateLastPublished,
  DATE(o.signedAt) AS DateSigned,
  DATE(o.archivedAt) AS DateArchived,
  DATE(o.lastModified) AS DateLastModified
FROM {{ source('Whetstone', 'Observations_raw')}} AS o
LEFT JOIN UNNEST(observer) AS ob
LEFT JOIN UNNEST(rubric) AS r
LEFT JOIN UNNEST(teacher) AS t
LEFT JOIN UNNEST(teachingAssignment) AS ta