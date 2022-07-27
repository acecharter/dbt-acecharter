WITH
  dash AS (
    SELECT * FROM {{ref ('int_CaDashAll')}}
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

  final AS (
    SELECT
      d.IndicatorName,
      e.EntityType,
      d.EntityCode,
      e.EntityName,
      e.EntityNameShort,
      d.Cds,
      d.RType,
      d.SchoolName,
      d.DistrictName,
      d.CountyName,
      d.CharterFlag,
      d.CoeFlag,
      d.DassFlag,
      d.StudentGroup,
      g.StudentGroupName, 
      d.CurrDenom,
      d.CurrStatus,
      d.PriorDenom,
      d.PriorStatus,
      d.Change,
      d.StatusLevel,
      d.ChangeLevel,
      d.Color,
      c.ColorName,
      d.Box,
      d.ReportingYear
    FROM dash AS d
    LEFT JOIN entities AS e
    ON d.EntityCode = e.EntityCode
    LEFT JOIN student_groups AS g
    ON d.StudentGroup = g.StudentGroup
    LEFT JOIN colors AS c
    ON d.Color = c.Color
  )

SELECT * FROM final