SELECT
  i._id AS QuickFeedbackId,
  i.shared AS SharedFeedback,
  i.private AS PrivateFeedback,
  u._id AS UserId,
  u.email AS UserEmail,
  u.name AS UserName,
  c._id AS CreatorId,
  c.email AS CreatorEmail,
  c.name AS CreatorName,
  DATE(i.created) AS DateCreated,
  DATE(i.lastModified) AS DateLastModified
FROM {{ source('Whetstone', 'Informals_raw')}} AS i
LEFT JOIN UNNEST(user) AS u
LEFT JOIN UNNEST(creator) AS c