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
        FROM {{ ref('base_RD__CaDashCci2019')}} 
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
        FROM {{ ref('base_RD__CaDashCci2018')}} 
    ),

    cci_2017 AS (
        SELECT
            Cds,
            RType,
            SchoolName,
            DistrictName,
            CountyName,
            CharterFlag,
            CoeFlag,
            CAST(NULL AS BOOL) AS DassFlag,
            CAST(NULL AS BOOL) AS SafetyNet,
            StudentGroup,
            CurrDenom,
            CurrStatus,
            CAST(NULL AS INT64) AS PriorDenom,
            CAST(NULL AS FLOAT64) AS PriorStatus,
            CAST(NULL AS FLOAT64) AS Change,
            StatusLevel,
            CAST(NULL AS INT64) AS ChangeLevel,
            CAST(NULL AS INT64) AS Color,
            CAST(NULL AS INT64) AS Box,
            CurrPrep,
            CurrPrepPct,
            CAST(NULL AS INT64) CurrPrepSummative,
            CAST(NULL AS FLOAT64) AS CurrPrepSummativePct,
            CAST(NULL AS INT64) CurrPrepApExam,
            CAST(NULL AS FLOAT64) AS CurrPrepApExamPct,
            CAST(NULL AS INT64) CurrPrepIbExam,
            CAST(NULL AS FLOAT64) AS CurrPrepIbExamPct,
            CAST(NULL AS INT64) CurrPrepCollegeCredit,
            CAST(NULL AS FLOAT64) AS CurrPrepCollegeCreditPct,
            CAST(NULL AS INT64) CurrPrepAgPlus,
            CAST(NULL AS FLOAT64) AS CurrPrepAgPlusPct,
            CAST(NULL AS INT64) CurrPrepCtePlus,
            CAST(NULL AS FLOAT64) AS CurrPrepCtePlusPct,
            CAST(NULL AS INT64) CurrPrepSsb,
            CAST(NULL AS FLOAT64) AS CurrPrepSsbPct,
            CAST(NULL AS INT64) CurrPrepMilSci,
            CAST(NULL AS FLOAT64) AS CurrPrepMilSciPct,
            CAST(NULL AS INT64) CurrAPrep,
            CAST(NULL AS FLOAT64) AS CurrAPrepPct,
            CAST(NULL AS INT64) CurrAPrepSummative,
            CAST(NULL AS FLOAT64) AS CurrAPrepSummativePct,
            CAST(NULL AS INT64) CurrAPrepCollegeCredit,
            CAST(NULL AS FLOAT64) AS CurrAPrepCollegeCreditPct,
            CAST(NULL AS INT64) CurrAPrepAg,
            CAST(NULL AS FLOAT64) AS CurrAPrepAgPct,
            CAST(NULL AS INT64) CurrAPrepCte,
            CAST(NULL AS FLOAT64) AS CurrAPrepCtePct,
            CAST(NULL AS INT64) CurrAPrepMilSci,
            CAST(NULL AS FLOAT64) AS CurrAPrepMilSciPct,
            CAST(NULL AS INT64) CurrNPrep,
            CAST(NULL AS FLOAT64) AS CurrNPrepPct,
            CAST(NULL AS INT64) PriorPrep,
            CAST(NULL AS FLOAT64) AS PriorPrepPct,
            CAST(NULL AS INT64) PriorPrepSummative,
            CAST(NULL AS FLOAT64) AS PriorPrepSummativePct,
            CAST(NULL AS INT64) PriorPrepApExam,
            CAST(NULL AS FLOAT64) AS PriorPrepApExamPct,
            CAST(NULL AS INT64) PriorPrepIbExam,
            CAST(NULL AS FLOAT64) AS PriorPrepIbExamPct,
            CAST(NULL AS INT64) PriorPrepCollegeCredit,
            CAST(NULL AS FLOAT64) AS PriorPrepCollegeCreditPct,
            CAST(NULL AS INT64) PriorPrepAgPlus,
            CAST(NULL AS FLOAT64) AS PriorPrepAgPlusPct,
            CAST(NULL AS INT64) PriorPrepCtePlus,
            CAST(NULL AS FLOAT64) AS PriorPrepCtePlusPct,
            CAST(NULL AS INT64) PriorPrepSsb,
            CAST(NULL AS FLOAT64) AS PriorPrepSsbPct,
            CAST(NULL AS INT64) PriorPrepMilSci,
            CAST(NULL AS FLOAT64) AS PriorPrepMilSciPct,
            CAST(NULL AS INT64) PriorAPrep,
            CAST(NULL AS FLOAT64) AS PriorAPrepPct,
            CAST(NULL AS INT64) PriorAPrepSummative,
            CAST(NULL AS FLOAT64) AS PriorAPrepSummativePct,
            CAST(NULL AS INT64) PriorAPrepCollegeCredit,
            CAST(NULL AS FLOAT64) AS PriorAPrepCollegeCreditPct,
            CAST(NULL AS INT64) PriorAPrepAg,
            CAST(NULL AS FLOAT64) AS PriorAPrepAgPct,
            CAST(NULL AS INT64) PriorAPrepCte,
            CAST(NULL AS FLOAT64) AS PriorAPrepCtePct,
            CAST(NULL AS INT64) PriorAPrepMilSci,
            CAST(NULL AS FLOAT64) AS PriorAPrepMilSciPct,
            CAST(NULL AS INT64) PriorNPrep,
            CAST(NULL AS FLOAT64) AS PriorNPrepPct,
            ReportingYear
        FROM {{ ref('base_RD__CaDashCci2017')}} 
    ),

    unioned AS (
        SELECT * FROM cci_2019
        UNION ALL
        SELECT * FROM cci_2018
        UNION ALL
        SELECT * FROM cci_2017
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
            IFNULL(e.EntityType, IF(u.Rtype = 'S', 'School', NULL)) AS EntityType,
            IFNULL(e.EntityName, u.SchoolName) AS EntityName,
            IFNULL(e.EntityNameShort, u.SchoolName) AS EntityNameShort,
            g.StudentGroupName,
            IFNULL(sl.StatusLevelName, 'No Status Level') AS StatusLevelName,
            IFNULL(cl.ChangeLevelName, 'No Change Level') AS ChangeLevelName,
            IFNULL(c.ColorName, 'No Color') AS ColorName,
            u.*
        FROM unioned_w_entity_codes AS u
        LEFT JOIN entities AS e
        ON u.EntityCode = e.EntityCode
        LEFT JOIN student_groups AS g
        ON u.StudentGroup = g.StudentGroup
        LEFT JOIN status_levels AS sl
        ON u.StatusLevel = sl.StatusLevel
        LEFT JOIN change_levels AS cl
        ON u.ChangeLevel = cl.ChangeLevel
        LEFT JOIN colors AS c
        ON u.Color = c.Color
    )

SELECT * FROM final
