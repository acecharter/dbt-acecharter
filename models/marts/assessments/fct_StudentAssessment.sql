WITH
  assessment_names AS (
    SELECT * FROM {{ ref('stg_GSD__Assessments') }}
  ),

  star AS (    
    SELECT
      s.AceAssessmentId,
      a.AssessmentNameShort AS AssessmentName,
      a.AssessmentSubject,
      s.StateUniqueId,
      s.TestedSchoolId,
      s.SchoolYear AS AssessmentSchoolYear,
      s.AssessmentID AS AssessmentId,
      CAST(s.AssessmentDate AS STRING) AS AssessmentDate,
      s.GradeLevel,
      s.AssessmentGradeLevel,
      'Overall' AS AssessmentObjective,
      s.ReportingMethod,
      s.StudentResultDataType,
      s.StudentResult
    FROM {{ ref('int_RenStar__2_melted') }} AS s
    LEFT JOIN assessment_names AS a
    USING (AceAssessmentId)
  ),

  cers AS (
    SELECT
      c.AceAssessmentId,
      CASE
        WHEN AceAssessmentId = '15' THEN CONCAT('SB ELA', REGEXP_EXTRACT(c.AssessmentName, '.+(\\s\\-\\s.+)'))
        WHEN AceAssessmentId = '16' THEN CONCAT('SB Math', REGEXP_EXTRACT(c.AssessmentName, '.+(\\s\\-\\s.+)'))
        ELSE a.AssessmentNameShort 
      END AS AssessmentName,
      a.AssessmentSubject,
      c.StateUniqueId,
      c.TestedSchoolId,
      c.SchoolYear AS AssessmentSchoolYear,
      c.AssessmentId,
      CAST(c.AssessmentDate AS STRING) AS AssessmentDate,
      c.GradeLevel,
      c.AssessmentGradeLevel,
      c.AssessmentObjective,
      c.ReportingMethod,
      c.StudentResultDataType,
      c.StudentResult
    FROM {{ ref('int_Cers__3_melted') }} AS c
    LEFT JOIN assessment_names AS a
    USING (AceAssessmentId)
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
      GradeLevel AS AssessmentGradeLevel,
      'Overall' AS AssessmentObjective,
      'Percent Score' AS ReportingMethod,
      'FLOAT64' AS StudentResultDataType,
      CAST(Score AS STRING) AS StudentResult
    FROM {{ ref('int_Anet__aggregated') }} AS c 
  ),

  unioned_results AS (
    SELECT * FROM star
    UNION ALL
    SELECT * FROM cers
    UNION ALL
    SELECT * FROM anet
  ),

  final AS (
    SELECT
      n.AceAssessmentId,
      r.AssessmentName,
      r.StateUniqueId,
      r.TestedSchoolId,
      r.AssessmentSchoolYear,
      r.AssessmentId,
      r.AssessmentDate,
      r.GradeLevel AS GradeLevelWhenAssessed,
      r.AssessmentGradeLevel,
      r.AssessmentSubject,
      r.AssessmentObjective,
      r.ReportingMethod,
      r.StudentResultDataType,
      r.StudentResult
    FROM unioned_results AS r
    LEFT JOIN assessment_names AS n
    USING (AceAssessmentId)
  )

SELECT * FROM final
WHERE AceAssessmentId = '14'
