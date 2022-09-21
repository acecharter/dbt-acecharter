select
  Student_ID AS StudentUniqueId,
  Amplify,
  DuoLingo,
  NoRedInk,
  Zearn,
  Khan
from {{ source('GoogleSheetData', 'EsperanzaStudentConferenceData')}}