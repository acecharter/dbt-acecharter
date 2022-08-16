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

  elpac_elpi_added AS(
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
      ElpiLevelMinus1,
      ElpiLevelNumericMinus1,
      ElpiLevelRankMinus1,
      ElpiLevelMinus2
      ElpiLevelNumericMinus2,
      ElpiLevelRankMinus2,
    FROM elpac_elpi_added
  )

SELECT * FROM final
