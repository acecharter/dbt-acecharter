select
  CAST(Student_ID as STRING) as StudentUniqueId,
  _2022_SBAC_ELA as SbacElaDfs22,
  ELA_Benchmark_Q1 as ElaDfsQ1,
  ELA_Benchmark_Q2 as ElaDfsQ2,
  ELA_Benchmark_Q3 as ElaDfsQ3,
  Amplify,
  NoRedInk,
  _2022_SBAC_Math as SbacMathDfs22,
  Math_Q1_Benchmark as MathDfsQ1,
  Math_Benchmark_Q2 as MathDfsQ2,
  Math_Benchmark_Q3 as MathDfsQ3,
  Zearn,
  Khan
from {{ source('GoogleSheetData', 'EsperanzaStudentConferenceData')}}