WITH
    susp_1718 AS (
        SELECT * FROM {{ source('RawData', 'CdeSusp1718')}}
    ),

    susp_1819 AS (
        SELECT * FROM {{ source('RawData', 'CdeSusp1819')}}
    ),

    susp_1920 AS (
        SELECT * FROM {{ source('RawData', 'CdeSusp1920')}}
    ),

    susp_2021 AS (
        SELECT * FROM {{ source('RawData', 'CdeSusp2021')}}
    ),

    unioned AS (
        SELECT * FROM susp_1718
        UNION ALL
        SELECT * FROM susp_1819
        UNION ALL
        SELECT * FROM susp_1920
        UNION ALL
        SELECT * FROM susp_2021
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
            TRIM(CharterYN) AS CharterSchool,
            ReportingCategory,
            CAST(NULLIF(Cumulative_Enrollment, '*') AS INT64) AS CumulativeEnrollment,
            CAST(NULLIF(Total_Suspensions, '*') AS INT64) AS TotalSuspensions,
            CAST(NULLIF(Unduplicated_Count_of_Students_Suspended__Total_, '*') AS INT64) AS UnduplicatedCountOfStudentsSuspendedTotal,
            CAST(NULLIF(Unduplicated_Count_of_Students_Suspended__Defiance_Only_, '*') AS INT64) AS UnduplicatedCountOfStudentsSuspendedDefianceOnly,
            ROUND(CAST(NULLIF(Suspension_Rate__Total_, '*') AS FLOAT64)/100, 3) AS SuspensionRateTotal,
            CAST(NULLIF(Suspension_Count_Violent_Incident__Injury_, '*') AS INT64) AS SuspensionCountViolentIncidentInjury,
            CAST(NULLIF(Suspension_Count_Violent_Incident__No_Injury_, '*') AS INT64) AS SuspensionCountViolentIncidentNoInjury,
            CAST(NULLIF(Suspension_Count_Weapons_Possession, '*') AS INT64) AS SuspensionCountWeaponsPossession,
            CAST(NULLIF(Suspension_Count_Illicit_Drug_Related, '*') AS INT64) AS SuspensionCountIllicitDrugRelated,
            CAST(NULLIF(Suspension_Count_Defiance_Only, '*') AS INT64) AS SuspensionCountDefianceOnly,
            CAST(NULLIF(Suspension_Count_Other_Reasons, '*') AS INT64) AS SuspensionCountOtherReasons
        FROM unioned
        WHERE
            AggregateLevel = 'T'
            OR (
                AggregateLevel = 'C' 
                AND CountyCode = 43
            )
            OR DistrictCode IN (
                10439, --SCCOE
                69369, --ARUSD
                69450, --FMSD
                69666, --SJUSD
                69427 -- ESUHSD
            )

    )

SELECT * FROM final