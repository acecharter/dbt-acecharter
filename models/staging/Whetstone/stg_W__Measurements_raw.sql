SELECTo
    m._id AS MeasurementId,
    m.name AS MeasurementName,
    o.label AS MeasurementOptionLabel,
    o.description AS MeasurementOptionDescription,
    DATE(created) AS DateCreated,
    DATE(lastModified) AS DateLastModified,
    DATE(m.archivedAt) AS DateArchived
FROM {{ source('Whetstone', 'Measurements_raw')}} AS m
LEFT JOIN UNNEST(measurementOptions) AS o