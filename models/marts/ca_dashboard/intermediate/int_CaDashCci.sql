WITH 
cci_2019 AS (
    SELECT
      Cds,
      RType,
      SchoolName,
      DistrictName,
      CountyName,
      CharterFlag,
      CoeFlag,
      DassFlag,
      SafetyNet,
      StudentGroup,
      CurrDenom,
      CurrStatus,
      PriorDenom,
      PriorStatus,
      Change,
      StatusLevel,
      ChangeLevel,
      Color,
      Box,
      CurrPrep,
      CurrPrepPct,
      CurrPrepSummative,
      CurrPrepSummativePct,
      CurrPrepApExam,
      CurrPrepApExamPct,
      CurrPrepIbExam,
      CurrPrepIbExamPct,
      CurrPrepCollegeCredit,
      CurrPrepCollegeCreditPct,
      CurrPrepAgPlus,
      CurrPrepAgPlusPct,
      CurrPrepCtePlus,
      CurrPrepCtePlusPct,
      CurrPrepSsb,
      CurrPrepSsbPct,
      CurrPrepMilSci,
      CurrPrepMilSciPct,
      CurrAPrep,
      CurrAPrepPct,
      CurrAPrepSummative,
      CurrAPrepSummativePct,
      CurrAPrepCollegeCredit,
      CurrAPrepCollegeCreditPct,
      CurrAPrepAg,
      CurrAPrepAgPct,
      CurrAPrepCte,
      CurrAPrepCtePct,
      CurrAPrepMilSci,
      CurrAPrepMilSciPct,
      CurrNPrep,
      CurrNPrepPct,
      PriorPrep,
      PriorPrepPct,
      PriorPrepSummative,
      PriorPrepSummativePct,
      PriorPrepApExam,
      PriorPrepApExamPct,
      PriorPrepIbExam,
      PriorPrepIbExamPct,
      PriorPrepCollegeCredit,
      PriorPrepCollegeCreditPct,
      PriorPrepAgPlus,
      PriorPrepAgPlusPct,
      PriorPrepCtePlus,
      PriorPrepCtePlusPct,
      PriorPrepSsb,
      PriorPrepSsbPct,
      PriorPrepMilSci,
      PriorPrepMilSciPct,
      PriorAPrep,
      PriorAPrepPct,
      PriorAPrepSummative,
      PriorAPrepSummativePct,
      PriorAPrepCollegeCredit,
      PriorAPrepCollegeCreditPct,
      PriorAPrepAg,
      PriorAPrepAgPct,
      PriorAPrepCte,
      PriorAPrepCtePct,
      PriorAPrepMilSci,
      PriorAPrepMilSciPct,
      PriorNPrep,
      PriorNPrepPct,
      ReportingYear
  FROM {{ ref('stg_RD__CaDashCci2019')}} 
  ),
  
  cci_2018 AS (
    SELECT
      Cds,
      RType,
      SchoolName,
      DistrictName,
      CountyName,
      CharterFlag,
      CoeFlag,
      DassFlag,
      CAST(NULL AS BOOL) AS SafetyNet,
      StudentGroup,
      CurrDenom,
      CurrStatus,
      PriorDenom,
      PriorStatus,
      Change,
      StatusLevel,
      ChangeLevel,
      Color,
      Box,
      CurrPrep,
      CurrPrepPct,
      CurrPrepSummative,
      CurrPrepSummativePct,
      CurrPrepApExam,
      CurrPrepApExamPct,
      CurrPrepIbExam,
      CurrPrepIbExamPct,
      CurrPrepCollegeCredit,
      CurrPrepCollegeCreditPct,
      CurrPrepAgPlus,
      CurrPrepAgPlusPct,
      CurrPrepCtePlus,
      CurrPrepCtePlusPct,
      CurrPrepSsb,
      CurrPrepSsbPct,
      CurrPrepMilSci,
      CurrPrepMilSciPct,
      CurrAPrep,
      CurrAPrepPct,
      CurrAPrepSummative,
      CurrAPrepSummativePct,
      CurrAPrepCollegeCredit,
      CurrAPrepCollegeCreditPct,
      CurrAPrepAg,
      CurrAPrepAgPct,
      CurrAPrepCte,
      CurrAPrepCtePct,
      CurrAPrepMilSci,
      CurrAPrepMilSciPct,
      CurrNPrep,
      CurrNPrepPct,
      PriorPrep,
      PriorPrepPct,
      PriorPrepSummative,
      PriorPrepSummativePct,
      PriorPrepApExam,
      PriorPrepApExamPct,
      PriorPrepIbExam,
      PriorPrepIbExamPct,
      PriorPrepCollegeCredit,
      PriorPrepCollegeCreditPct,
      PriorPrepAgPlus,
      PriorPrepAgPlusPct,
      PriorPrepCtePlus,
      PriorPrepCtePlusPct,
      PriorPrepSsb,
      PriorPrepSsbPct,
      PriorPrepMilSci,
      PriorPrepMilSciPct,
      PriorAPrep,
      PriorAPrepPct,
      PriorAPrepSummative,
      PriorAPrepSummativePct,
      PriorAPrepCollegeCredit,
      PriorAPrepCollegeCreditPct,
      PriorAPrepAg,
      PriorAPrepAgPct,
      PriorAPrepCte,
      PriorAPrepCtePct,
      PriorAPrepMilSci,
      PriorAPrepMilSciPct,
      PriorNPrep,
      PriorNPrepPct,
      ReportingYear
    FROM {{ ref('stg_RD__CaDashCci2018')}} 
  ),

  unioned AS (
    SELECT * FROM cci_2018
    UNION ALL
    SELECT * FROM cci_2019
  ),

  unioned_w_entity_codes AS (
    SELECT
      CASE
        WHEN RType = 'X' THEN '00'
        WHEN RType = 'D' THEN SUBSTR(cds, 3, 5)
        WHEN RType = 'S' THEN SUBSTR(cds, LENGTH(cds)-6, 7)
      END AS EntityCode,
      *
    FROM unioned
  ),
  
  entities AS (
    SELECT * FROM {{ ref('dim_Entities')}}
  ),

  codes AS (
    SELECT * FROM {{ ref('stg_GSD__CaDashCodes')}}
  ),

  student_groups AS (
    SELECT
      Code AS StudentGroup,
      Value AS StudentGroupName
    FROM codes
    WHERE CodeColumn = 'StudentGroup'
  ),

  colors AS (
    SELECT
      CAST(Code AS INT64) AS Color,
      Value AS ColorName
    FROM codes
    WHERE CodeColumn = 'Color'
  ),

  status_levels AS (
    SELECT
      CAST(Code AS INT64) AS StatusLevel,
      Value AS StatusLevelName
    FROM codes
    WHERE CodeColumn = 'StatusLevel - College/Career'
  ),

  change_levels AS (
    SELECT
      CAST(Code AS INT64) AS ChangeLevel,
      Value AS ChangeLevelName
    FROM codes
    WHERE CodeColumn = 'ChangeLevel - College/Career'
  ),

  final AS (
    SELECT
      'College/Career' AS IndicatorName,
      e.EntityType,
      e.EntityName,
      e.EntityNameShort,
      g.StudentGroupName,
      c.ColorName,
      u.*
    FROM unioned_w_entity_codes AS u
    LEFT JOIN entities AS e
    ON u.EntityCode = e.EntityCode
    LEFT JOIN student_groups AS g
    ON u.StudentGroup = g.StudentGroup
    LEFT JOIN colors AS c
    ON u.Color = c.Color
  )

SELECT * FROM final
