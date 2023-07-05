select
    DatasetName,
    TableName,
    date(DateTableLastUpdated) as DateTableLastUpdated
from {{ source('GoogleSheetData', 'ManuallyMaintainedFilesTracker') }}
where TableName is not null
