select * from {{ source('GoogleSheetData', 'CourseSubjects') }}
