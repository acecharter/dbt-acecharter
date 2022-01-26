SELECT
  DatasetName,
  TableName,
  DATE(DateTableLastUpdated) AS DateTableLastUpdated
FROM {{ source('GoogleSheetData', 'ManuallyMaintainedFilesTracker')}}
WHERE TableName IS NOT NULL