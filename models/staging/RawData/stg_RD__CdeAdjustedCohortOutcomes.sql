WITH
    acgr_2017 AS (
        SELECT * FROM {{ source('RawData', 'CdeAdjustedCohortOutcomes2017')}}
    ),

    acgr_2018 AS (
        SELECT * FROM {{ source('RawData', 'CdeAdjustedCohortOutcomes2018')}}
    ),

    acgr_2019 AS (
        SELECT * FROM {{ source('RawData', 'CdeAdjustedCohortOutcomes2019')}}
    ),

    acgr_2020 AS (
        SELECT * FROM {{ source('RawData', 'CdeAdjustedCohortOutcomes2020')}}
    ),

    acgr_2021 AS (
        SELECT * FROM {{ source('RawData', 'CdeAdjustedCohortOutcomes2021')}}
    ),

    acgr_2022 AS (
        SELECT * FROM {{ ref('base_RD__CdeAdjustedCohortOutcomes2022')}}
    ),

    unioned AS (
        SELECT * FROM acgr_2017
        UNION ALL
        SELECT * FROM acgr_2018
        UNION ALL
        SELECT * FROM acgr_2019
        UNION ALL
        SELECT * FROM acgr_2020
        UNION ALL
        SELECT * FROM acgr_2021
        UNION ALL
        SELECT * FROM acgr_2022
    ),

    final AS (
        SELECT
            AcademicYear,
            AggregateLevel,
            CASE AggregateLevel
                WHEN 'T' THEN 'State'
                WHEN 'C' THEN 'County'
                WHEN 'D' THEN 'District'
                WHEN 'S' THEN 'School'
            END AS EntityType,
            FORMAT("%02d", CAST(CountyCode AS INT64)) AS CountyCode,
            FORMAT("%05d", CAST(DistrictCode AS INT64)) AS DistrictCode,
            FORMAT("%07d", CAST(SchoolCode AS INT64)) AS SchoolCode,
            CountyName,
            DistrictName,
            SchoolName,
            TRIM(CharterSchool) AS CharterSchool,
            TRIM(DASS) AS DASS,
            ReportingCategory,
            CohortStudents,
            Regular_HS_Diploma_Graduates__Count_ AS RegularHsDiplomaGraduatesCount,
            Regular_HS_Diploma_Graduates__Rate_ AS RegularHsDiplomaGraduatesRate,
            Met_UC_CSU_Grad_Req_s__Count_ AS MetUcCsuGradReqsCount,
            Met_UC_CSU_Grad_Req_s__Rate_ AS MetUcCsuGradReqsRate,
            Seal_of_Biliteracy__Count_ AS SealOfBiliteracyCount,
            Seal_of_Biliteracy__Rate_ AS SealOfBiliteracyRate,
            Golden_State_Seal_Merit_Diploma__Count_ AS GoldenStateSealMeritDiplomaCount,
            Golden_State_Seal_Merit_Diploma__Rate AS GoldenStateSealMeritDiplomaRate,
            CHSPE_Completer__Count_ AS ChspeCompleterCount,
            CHSPE_Completer__Rate_ AS ChspeCompleterRate,
            Adult_Ed__HS_Diploma__Count_ AS AdultEdHsDiplomaCount,
            Adult_Ed__HS_Diploma__Rate_ AS AdultEdHsDiplomaRate,
            SPED_Certificate__Count_ AS SpedCertificateCount,
            SPED_Certificate__Rate_ AS SpedCertificateRate,
            GED_Completer__Count_ AS GedCompleterCount,
            GED_Completer__Rate_ AS GedCompleterRate,
            Other_Transfer__Count_ AS OtherTransferCount,
            Other_Transfer__Rate_ AS OtherTransferRate,
            Dropout__Count_ AS DropoutCount,
            Dropout__Rate_ AS DropoutRate,
            Still_Enrolled__Count_ AS StillEnrolledCount,
            Still_Enrolled__Rate_ AS StillEnrolledRate
        FROM unioned
        WHERE
            AggregateLevel = 'T'
            OR (
                AggregateLevel = 'C' 
                AND CountyCode = 43
            )
            OR DistrictCode = 69427
    )

SELECT * FROM final