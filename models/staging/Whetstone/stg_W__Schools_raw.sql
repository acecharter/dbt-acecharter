SELECT
  s._id AS SchoolId,
  s.name AS SchoolName,
  s.abbreviation AS SchoolNameAbbreviation,
  a._id AS AdminId,
  a.email AS AdminEmail,
  a.name AS AdminName,
  aa._id AS AssistantAdminId,
  aa.email AS AssistandAdminEmail,
  aa.name AS AssistantAdminName,
  n._id AS NonInstructionalAdminId,
  n.email AS NonInstructionalAdminEmail,
  n.name AS NonInstructionalAdminName,
  g._id AS ObservationGroupId,
  g.name AS ObservationGroupName,
  g.locked AS ObservationGroupLocked,
  oe._id AS ObserveeId,
  oe.email AS ObserveeEmail,
  oe.name AS ObserveeName,
  ob._id AS ObserverId,
  ob.email AS ObserverEmail,
  ob.name AS ObserverName,
  DATE(s.created) AS DateCreated,
  DATE(s.archivedAt) AS DateArchived,
  DATE(s.lastModified) AS DateLastModified
FROM {{ source('Whetstone', 'Schools_raw')}} AS s
LEFT JOIN UNNEST(admins) AS a
LEFT JOIN UNNEST(assistantAdmins) AS aa
LEFT JOIN UNNEST(nonInstructionalAdmins) AS n
LEFT JOIN UNNEST(observationGroups) AS g
LEFT JOIN UNNEST(g.observees) AS oe
LEFT JOIN UNNEST(g.observers) AS ob