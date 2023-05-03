SELECT
    '2021-22' AS SchoolYear,
    r._id AS RubricId,
    r.name AS RubricName,
    r.isPrivate AS IsPrivate,
    r.isPublished AS IsPublished,
    g._id AS MeasurementGroupId,
    g.key AS MeasurementGroupKey,
    g.name AS MeasurementGroupName,
    g.weight AS MeasurementGroupWeight,
    m._id AS MeasurmentId,
    m.key AS MeasurementKey,
    m.weight AS MeasurementWeight,
    m.exclude AS MeasurementExclude,
    m.require AS MeasurementRequire,
    m.isPrivate AS MeasurementIsPrivate,
    m.isCompetency AS MeasurementIsCompetency,
    m.measurement AS Measurement,
    DATE(created) AS DateCreated,
    DATE(r.archivedAt) AS DateArchived
 FROM {{ source('Whetstone_Archive', 'Rubrics_raw_SY22')}} AS r
 LEFT JOIN UNNEST(measurementGroups) AS g
 LEFT JOIN UNNEST(g.measurements) AS m