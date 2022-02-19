WITH 
  outcomes AS (
    SELECT
      *,
       CASE
        WHEN AggregateLevel = 'T' THEN '0'
        WHEN AggregateLevel = 'C' THEN CountyCode
        WHEN AggregateLevel = 'D' THEN DistrictCode
        WHEN AggregateLevel = 'S' THEN SchoolCode
      END AS EntityCode,     
    FROM {{ ref('stg_RD__CdeAdjustedCohortOutcomes')}}
  ),

  reg_grad AS (
    SELECT
      AcademicYear,
      AggregateLevel,
      EntityCode,
      CharterSchool,
      DASS,
      ReportingCategory,
      'Cohort Outcome' AS OutcomeType,
      'Regular HS Diploma' AS Outcome,
      RegularHsDiplomaGraduatesCount AS OutcomeCount
    FROM outcomes
  ),

  met_uc_csu AS (
    SELECT
      AcademicYear,
      AggregateLevel,
      EntityCode,
      CharterSchool,
      DASS,
      ReportingCategory,
      'Cohort Graduate Outcome' AS OutcomeType,
      'Met UC/CSU Grad Requirements' AS Outcome,
      MetUcCsuGradReqsCount AS OutcomeCount
    FROM outcomes
  ),

  seal_of_biliteracy AS (
    SELECT
      AcademicYear,
      AggregateLevel,
      EntityCode,
      CharterSchool,
      DASS,
      ReportingCategory,
      'Cohort Graduate Outcome' AS OutcomeType,
      'Seal of Biliteracy' AS Outcome,
      SealOfBiliteracyCount AS OutcomeCount
    FROM outcomes
  ),

  golden_state_seal AS (
    SELECT
      AcademicYear,
      AggregateLevel,
      EntityCode,
      CharterSchool,
      DASS,
      ReportingCategory,
      'Cohort Graduate Outcome' AS OutcomeType,
      'Golden State Seal Merit Diploma' AS Outcome,
      GoldenStateSealMeritDiplomaCount AS OutcomeCount
    FROM outcomes
  ),

  chspe_competer AS (
    SELECT
      AcademicYear,
      AggregateLevel,
      EntityCode,
      CharterSchool,
      DASS,
      ReportingCategory,
      'Cohort Outcome' AS OutcomeType,
      'CHSPE Completer' AS Outcome,
      ChspeCompleterCount AS OutcomeCount
    FROM outcomes
  ),

  adult_ed AS (
    SELECT
      AcademicYear,
      AggregateLevel,
      EntityCode,
      CharterSchool,
      DASS,
      ReportingCategory,
      'Cohort Outcome' AS OutcomeType,
      'Adult Ed HS Diploma' AS Outcome,
      AdultEdHsDiplomaCount AS OutcomeCount
    FROM outcomes
  ),

  sped AS (
    SELECT
      AcademicYear,
      AggregateLevel,
      EntityCode,
      CharterSchool,
      DASS,
      ReportingCategory,
      'Cohort Outcome' AS OutcomeType,
      'SPED Certificate' AS Outcome,
      SpedCertificateCount AS OutcomeCount
    FROM outcomes
  ),

  ged AS (
    SELECT
      AcademicYear,
      AggregateLevel,
      EntityCode,
      CharterSchool,
      DASS,
      ReportingCategory,
      'Cohort Outcome' AS OutcomeType,
      'GED Completer' AS Outcome,
      GedCompleterCount AS OutcomeCount
    FROM outcomes
  ),

  other AS (
    SELECT
      AcademicYear,
      AggregateLevel,
      EntityCode,
      CharterSchool,
      DASS,
      ReportingCategory,
      'Cohort Outcome' AS OutcomeType,
      'Other Transfer' AS Outcome,
      OtherTransferCount AS OutcomeCount
    FROM outcomes
  ),

  dropout AS (
    SELECT
      AcademicYear,
      AggregateLevel,
      EntityCode,
      CharterSchool,
      DASS,
      ReportingCategory,
      'Cohort Outcome' AS OutcomeType,
      'Dropout' AS Outcome,
      DropoutCount AS OutcomeCount
    FROM outcomes
  ),

  still_enrolled AS (
    SELECT
      AcademicYear,
      AggregateLevel,
      EntityCode,
      CharterSchool,
      DASS,
      ReportingCategory,
      'Cohort Outcome' AS OutcomeType,
      'Still Enrolled' AS Outcome,
      StillEnrolledCount AS OutcomeCount
    FROM outcomes
  ),

  unioned AS (
    SELECT * FROM reg_grad
    UNION ALL
    SELECT * FROM met_uc_csu
    UNION ALL
    SELECT * FROM seal_of_biliteracy
    UNION ALL
    SELECT * FROM golden_state_seal
    UNION ALL
    SELECT * FROM chspe_competer
    UNION ALL
    SELECT * FROM adult_ed
    UNION ALL
    SELECT * FROM sped
    UNION ALL
    SELECT * FROM ged
    UNION ALL
    SELECT * FROM other
    UNION ALL
    SELECT * FROM dropout
    UNION ALL
    SELECT * FROM still_enrolled    
  ),

final AS (
  SELECT *
  FROM unioned
  ORDER BY 1, 2, 3, 4, 5, 6, 7
)


SELECT * FROM final
WHERE OutcomeCount > 0
