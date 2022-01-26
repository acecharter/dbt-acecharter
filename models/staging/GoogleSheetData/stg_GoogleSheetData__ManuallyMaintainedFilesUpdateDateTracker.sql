SELECT
  DatasetName,
  TableName,
  DATE(DateOfLastUpdate) AS DateTableLastUpdated
FROM {{ source('GoogleSheetData', 'ManuallyMaintainedFilesUpdateDateTracker')}}
WHERE TableName IS NOT NULL