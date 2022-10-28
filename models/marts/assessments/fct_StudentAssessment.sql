WITH
  star AS (    
    SELECT
      s.AceAssessmentId,
      s.AssessmentName,
      s.AssessmentSubject,
      s.StateUniqueId,
      s.TestedSchoolId,
      s.SchoolYear AS AssessmentSchoolYear,
      s.AssessmentID AS AssessmentId,
      CAST(s.AssessmentDate AS STRING) AS AssessmentDate,
      s.GradeLevel AS GradeLevelWhenAssessed,
      CAST(s.AssessmentGradeLevel AS STRING) AS AssessmentGradeLevel,
      'Overall' AS AssessmentObjective,
      s.ReportingMethod,
      s.StudentResultDataType,
      s.StudentResult
    FROM {{ ref('int_RenStar_melted') }} AS s
  ),

  cers AS (
    SELECT
      AceAssessmentId,
      CASE
        WHEN AceAssessmentId = '1' THEN 'SB ELA Summative'
        WHEN AceAssessmentId = '2' THEN 'SB Math Summative'
        WHEN AceAssessmentId = '3' THEN 'CAA ELA'
        WHEN AceAssessmentId = '4' THEN 'CAA Math'
        WHEN AceAssessmentId = '5' THEN 'CAA Science'
        WHEN AceAssessmentId = '6' THEN 'CAST'
        WHEN AceAssessmentId = '7' THEN 'CSA'
        WHEN AceAssessmentId = '8' THEN 'Summative ELPAC'
        WHEN AceAssessmentId = '9' THEN 'Initial ELPAC'
        WHEN AceAssessmentId = '15' THEN CONCAT('SB ELA', REGEXP_EXTRACT(AssessmentName, '.+(\\s\\-\\s.+)'))
        WHEN AceAssessmentId = '16' THEN CONCAT('SB Math', REGEXP_EXTRACT(AssessmentName, '.+(\\s\\-\\s.+)'))
        WHEN AceAssessmentId = '17' THEN 'SB ELA ICA'
        WHEN AceAssessmentId = '18' THEN 'SB Math ICA'
      END AS AssessmentName,
      CASE
        WHEN AceAssessmentId IN ('1','3','15','17') THEN 'ELA'
        WHEN AceAssessmentId IN ('2','4','16','18') THEN 'Math'
        WHEN AceAssessmentId IN ('5','6') THEN 'Science'
        WHEN AceAssessmentId IN ('7') THEN 'Spanish'
        WHEN AceAssessmentId IN ('8','9') THEN 'English Fluency'
      END AS AssessmentSubject,
      StateUniqueId,
      TestedSchoolId,
      SchoolYear AS AssessmentSchoolYear,
      AssessmentId,
      CAST(AssessmentDate AS STRING) AS AssessmentDate,
      GradeLevelWhenAssessed,
      AssessmentGradeLevel,
      AssessmentObjective,
      ReportingMethod,
      StudentResultDataType,
      StudentResult
    FROM {{ ref('int_Cers__elpi_dfs_added_and_melted') }}
  ),

  anet AS (
    SELECT
      AceAssessmentId,
      CONCAT(AceAssessmentName, ' (Cycle ', CAST(Cycle AS STRING), ')') AS AssessmentName,
      Subject AS AssessmentSubject,
      StateUniqueId,
      StateSchoolCode AS TestedSchoolId,
      SchoolYear AS AssessmentSchoolYear,
      CONCAT(AssessmentName,'-',StateUniqueId) AS AssessmentId,
      CAST(NULL AS STRING) AS AssessmentDate,
      GradeLevel AS GradeLevelWhenAssessed,
      CAST(GradeLevel AS STRING) AS AssessmentGradeLevel,
      'Overall' AS AssessmentObjective,
      'Percent Score' AS ReportingMethod,
      'FLOAT64' AS StudentResultDataType,
      CAST(Score AS STRING) AS StudentResult
    FROM {{ ref('int_Anet__aggregated') }} AS c 
  ),

  amplify AS (
    SELECT
      AceAssessmentId,
      AceAssessmentName AS AssessmentName,
      Subject AS AssessmentSubject,
      StateUniqueId,
      SchoolId AS TestedSchoolId,
      SchoolYear AS AssessmentSchoolYear,
      CONCAT(ElaLessonTitle,'-',StateUniqueId) AS AssessmentId,
      ElaHandInDate AS AssessmentDate,
      GradeLevel AS GradeLevelWhenAssessed,
      GradeLevel AS AssessmentGradeLevel,
      'Overall' AS AssessmentObjective,
      'Percent Score' AS ReportingMethod,
      'FLOAT64' AS StudentResultDataType,
      CAST(ElaTestScore AS STRING) AS StudentResult
    FROM {{ ref('stg_Amplify') }} AS c 
  ),

  unioned_results AS (
    SELECT * FROM star
    UNION ALL
    SELECT * FROM cers
    UNION ALL
    SELECT * FROM anet
    UNION ALL
    SELECT * FROM amplify
  ),

  final AS (
    SELECT
      AceAssessmentId,
      AssessmentName,
      StateUniqueId,
      TestedSchoolId,
      AssessmentSchoolYear,
      AssessmentId,
      AssessmentDate,
      GradeLevelWhenAssessed,
      AssessmentGradeLevel,
      AssessmentSubject,
      AssessmentObjective,
      ReportingMethod,
      StudentResultDataType,
      StudentResult
    FROM unioned_results
  )

SELECT * FROM final
