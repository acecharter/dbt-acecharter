WITH
  elpac AS (
    SELECT * FROM {{ ref('base_RD__TomsElpacTested2022')}}
    UNION ALL SELECT * FROM {{ ref('base_RD__TomsElpacTested2021')}}
  ),

  assessment_ids AS (
    SELECT 
      AceAssessmentId,
      AssessmentNameShort AS AceAssessmentName,
      AssessmentSubject,
      SystemOrVendorAssessmentId
    FROM {{ ref('stg_GSD__Assessments') }}
    WHERE SystemOrVendorName = 'ELPAC'
  ),

  elpac_id_added AS (
    SELECT
      a.AceAssessmentId,
      a.AceAssessmentName,
      a.AssessmentSubject,
      e.*
    FROM elpac AS e
    LEFT JOIN assessment_ids AS a
    ON e.RecordType = a.SystemOrVendorAssessmentId
  ),

  elpi_levels AS (
    SELECT * FROM {{ ref('stg_GSD__ElpiLevels') }}
  ),

  elpac_elpi_level_added AS(
    SELECT
      r.*,
      e.ElpiLevel,
      e.ElpiLevelNumeric,
      e.ElpiLevelRank,
      e1.ElpiLevel AS ElpiLevelMinus1,
      e1.ElpiLevelNumeric AS ElpiLevelNumericMinus1,
      e1.ElpiLevelRank As ElpiLevelRankMinus1,
      e2.ElpiLevel AS ElpiLevelMinus2,
      e2.ElpiLevelNumeric AS ElpiLevelNumericMinus2,
      e2.ElpiLevelRank As ElpiLevelRankMinus2,
      e3.ElpiLevel AS ElpiLevelMinus3,
      e3.ElpiLevelNumeric AS ElpiLevelNumericMinus3,
      e3.ElpiLevelRank As ElpiLevelRankMinus3,
    FROM elpac_id_added AS r
    LEFT JOIN elpi_levels AS e
    ON 
      r.GradeAssessed = CAST(e.GradeLevel AS STRING) AND
      CAST(r.OverallScaleScore AS INT64) BETWEEN CAST(e.MinScaleScore AS INT64) AND CAST(e.MaxScaleScore AS INT64)
    LEFT JOIN elpi_levels AS e1
    ON 
      r.GradeAssessedMinus1 = CAST(e1.GradeLevel AS STRING) AND
      CAST(r.OverallScaleScoreMinus1 AS INT64) BETWEEN CAST(e1.MinScaleScore AS INT64) AND CAST(e1.MaxScaleScore AS INT64)
    LEFT JOIN elpi_levels AS e2
    ON 
      r.GradeAssessedMinus2 = CAST(e2.GradeLevel AS STRING) AND
      CAST(r.OverallScaleScoreMinus2 AS INT64) BETWEEN CAST(e2.MinScaleScore AS INT64) AND CAST(e2.MaxScaleScore AS INT64)
    LEFT JOIN elpi_levels AS e3
    ON 
      r.GradeAssessedMinus3 = CAST(e3.GradeLevel AS STRING) AND
      CAST(r.OverallScaleScoreMinus3 AS INT64) BETWEEN CAST(e3.MinScaleScore AS INT64) AND CAST(e3.MaxScaleScore AS INT64)
  ),

  elpi_change_added AS (
    SELECT
      *,
      ElpiLevelRank - ElpiLevelRankMinus1 AS ElpiLevelChange,
      CASE
        WHEN ElpiLevelNumeric = ElpiLevelNumeric THEN CONCAT('Maintained at ', ElpiLevel)
        When ElpiLevelNumeric > ElpiLevelNumericMinus1 THEN 'Increased'
        WHEN ElpiLevelNumeric < ElpiLevelNumericMinus1 THEN 'Declined'
      END AS ElpiChangeCategory,
      (ElpiLevelNumeric = 4 AND ElpiLevelNumericMinus1 = 4) OR ElpiLevelNumeric - ElpiLevelNumericMinus1 > 0 AS ElpiProgress,
      ElpiLevelRankMinus1 - ElpiLevelRankMinus2 AS ElpiLevelChangeMinus1,
      CASE
        WHEN ElpiLevelNumericMinus1 = ElpiLevelNumericMinus2 THEN CONCAT('Maintained at ', ElpiLevelMinus1)
        When ElpiLevelNumericMinus1 > ElpiLevelNumericMinus2 THEN 'Increased'
        WHEN ElpiLevelNumericMinus1 < ElpiLevelNumericMinus2 THEN 'Declined'
      END AS ElpiChangeCategoryMinus1,
      (ElpiLevelNumericMinus1 = 4 AND ElpiLevelNumericMinus2 = 4) OR ElpiLevelNumericMinus1 - ElpiLevelNumericMinus2 > 0 AS ElpiProgressMinus1,
      ElpiLevelRankMinus2 - ElpiLevelRankMinus3 AS ElpiLevelChangeMinus2,
      CASE
        WHEN ElpiLevelNumericMinus2 = ElpiLevelNumericMinus3 THEN CONCAT('Maintained at ', ElpiLevelMinus2)
        When ElpiLevelNumericMinus2 > ElpiLevelNumericMinus3 THEN 'Increased'
        WHEN ElpiLevelNumericMinus2 < ElpiLevelNumericMinus3 THEN 'Declined'
      END AS ElpiChangeCategoryMinus2,
      (ElpiLevelNumericMinus2 = 4 AND ElpiLevelNumericMinus3 = 4) OR ElpiLevelNumericMinus2 - ElpiLevelNumericMinus3 > 0 AS ElpiProgressMinus2,
    FROM elpac_elpi_level_added
  ),

 final AS (
   SELECT
      AceAssessmentId,
      AceAssessmentName,
      AssessmentSubject,
      RecordType,
      TestYear,
      SchoolYear,
      SSID,
      StudentLastName,
      StudentFirstName,
      StudentMiddleName,
      DateofBirth,
      Gender,
      GradeAssessed,
      CALPADSSchoolCode,
      CALPADSSchoolName,
      Section504Status,
      CALPADSIDEAIndicator,
      IDEAIndicatorForTesting,
      PrimaryDisabilityType,
      PrimaryDisabilityforTesting,
      MigrantStatus,
      ELEntryDate,
      ELExitDate,
      ELStatus,
      FirstEntryDateInUSSchool,
      ELASforTesting,
      CALPADSPrimaryLanguage,
      PrimaryLanguageforTesting,
      CEDSLanguageCode,
      MilitaryStatus,
      FosterStatus,
      HomelessStatus,
      EconomicDisadvantageStatus,
      EconomicDisadvantageTesting,
      ReportingEthnicity,
      ParentEducationLevel,
      TestedStatus,
      FinalTestedSchoolCode,
      FinalTestedCompletedDate,
      StudentExitCode,
      StudentExitWithdrawalDate,
      StudentRemovedCALPADSFileDate,
      ConditionCode,
      Attemptedness,
      IncludeIndicator,
      OverallScaleScore,
      OverallPL,
      OralLanguagePL,
      WrittenLanguagePL,
      ListeningPL,
      SpeakingPL,
      WritingPL,
      AttemptednessMinus1,
      GradeAssessedMinus1,
      OverallScaleScoreMinus1,
      OverallPLMinus1,
      OralLanguagePLMinus1,
      WrittenLanguagePLMinus1,
      ListeningPLMinus1,
      SpeakingPLMinus1,
      ReadingPLMinus1,
      WritingPLMinus1,
      AttemptednessMinus2,
      GradeAssessedMinus2,
      OverallScaleScoreMinus2,
      OverallPLMinus2,
      OralLanguagePLMinus2,
      WrittenLanguagePLMinus2,
      ListeningPLMinus2,
      SpeakingPLMinus2,
      ReadingPLMinus2,
      WritingPLMinus2,
      AttemptednessMinus3,
      GradeAssessedMinus3,
      OverallScaleScoreMinus3,
      OverallPLMinus3,
      OralLanguagePLMinus3,
      WrittenLanguagePLMinus3,
      ListeningPLMinus3,
      SpeakingPLMinus3,
      ReadingPLMinus3,
      WritingPLMinus3,
      ElpiLevel,
      ElpiLevelNumeric,
      ElpiLevelRank,
      ElpiLevelChange,
      ElpiChangeCategory,
      ElpiProgress,
      ElpiLevelMinus1,
      ElpiLevelNumericMinus1,
      ElpiLevelRankMinus1,
      ElpiLevelChangeMinus1,
      ElpiChangeCategoryMinus1,
      ElpiProgressMinus1,
      ElpiLevelMinus2,
      ElpiLevelNumericMinus2,
      ElpiLevelRankMinus2,
      ElpiLevelChangeMinus2,
      ElpiChangeCategoryMinus2,
      ElpiProgressMinus2,
      ElpiLevelMinus3,
      ElpiLevelNumericMinus3,
      ElpiLevelRankMinus3
    FROM elpi_change_added
  )

SELECT * FROM final
