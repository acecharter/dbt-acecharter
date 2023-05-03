SELECT
  '2021-22' AS SchoolYear,
  m._id AS MeasurementId,
  m.name AS MeasurementName,
  o.label AS MeasurementOptionLabel,
  o.description AS MeasurementOptionDescription,
  DATE(created) AS DateCreated,
  DATE(lastModified) AS DateLastModified,
  DATE(m.archivedAt) AS DateArchived
FROM {{ source('Whetstone_Archive', 'Measurements_raw_SY22')}} AS m
LEFT JOIN UNNEST(measurementOptions) AS o