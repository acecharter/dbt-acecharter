WITH
    caaspp AS (
        SELECT * FROM {{ ref('base_RD__TomsCaasppTested2023')}}
        UNION ALL
        SELECT * FROM {{ ref('base_RD__TomsCaasppTested2022')}}
    ),
    
    race_ethnicity AS (
        SELECT * FROM {{ ref('stg_RD__TomsEthnicityCodes')}}
    ),

    assessment_ids AS (
        SELECT 
            AceAssessmentId,
            AssessmentNameShort AS AceAssessmentName,
            AssessmentSubject,
            SystemOrVendorAssessmentId
        FROM {{ ref('stg_GSD__Assessments') }}
        WHERE SystemOrVendorName = 'CAASPP'
    ),
    
    min_met_scores AS (
        SELECT
            AceAssessmentId,
            GradeLevel,
            MinStandardMetScaleScore
        FROM {{ ref('fct_CaasppMinMetScaleScores') }}
        WHERE Area='Overall'
    ),

    caaspp_id_race_added AS (
        SELECT
            a.AceAssessmentId,
            a.AceAssessmentName,
            a.AssessmentSubject,
            c.*,
            r.RaceEthnicity
        FROM caaspp AS c
        LEFT JOIN assessment_ids AS a
        ON c.RecordType = a.SystemOrVendorAssessmentId
        LEFT JOIN race_ethnicity AS r
        ON c.ReportingEthnicity = r.RaceEthnicityCode
    ),

    final AS (
        SELECT
            c.*,
            CASE WHEN ELStatus IS TRUE OR DATE(RFEPDate) > DATE(TestYear - 4, 6, 15) THEN TRUE ELSE FALSE END AS ElWithinPast4Years,
            CAST(
                CASE WHEN c.ScaleScore IS NOT NULL THEN ROUND(c.ScaleScore - m.MinStandardMetScaleScore, 0) ELSE NULL END AS INT64
            ) AS Dfs,
            CAST(
                CASE WHEN c.ScaleScoreMinus1 IS NOT NULL THEN ROUND(c.ScaleScoreMinus1 - m1.MinStandardMetScaleScore, 0) ELSE NULL END AS INT64
            ) AS DfsMinus1,
            CAST(
                CASE WHEN c.ScaleScoreMinus2 IS NOT NULL THEN ROUND(c.ScaleScoreMinus2 - m2.MinStandardMetScaleScore, 0) ELSE NULL END AS INT64
            ) AS DfsMinus2,
            CAST(
                CASE WHEN c.ScaleScoreMinus3 IS NOT NULL THEN ROUND(c.ScaleScoreMinus3 - m3.MinStandardMetScaleScore, 0) ELSE NULL END AS INT64
            ) AS DfsMinus3,
        FROM caaspp_id_race_added AS c
        LEFT JOIN min_met_scores AS m
        ON
            c.AceAssessmentId = m.AceAssessmentId
            AND CAST(c.GradeAssessed AS STRING) = m.GradeLevel
        LEFT JOIN min_met_scores AS m1
        ON
            c.AceAssessmentId = m1.AceAssessmentId
            AND CAST(c.GradeAssessedMinus1 AS STRING) = m1.GradeLevel
        LEFT JOIN min_met_scores AS m2
        ON
            c.AceAssessmentId = m2.AceAssessmentId
            AND CAST(c.GradeAssessedMinus2 AS STRING) = m2.GradeLevel
        LEFT JOIN min_met_scores AS m3
        ON
            c.AceAssessmentId = m3.AceAssessmentId
            AND CAST(c.GradeAssessedMinus3 AS STRING) = m3.GradeLevel
    )

SELECT * FROM final