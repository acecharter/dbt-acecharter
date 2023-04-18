SELECT
    FORMAT("%02d", CountyCode) AS CountyCode,
    FORMAT("%05d", DistrictCode) AS DistrictCode,
    FORMAT("%07d", SchoolCode) AS SchoolCode,
    FORMAT("%02d", RecordType) AS RecordType,
    CAST(CharterNumber AS STRING) AS CharterNumber,
    CAST(TestYear AS INT64) AS TestYear,
    CAST(StudentGroupID AS STRING) AS StudentGroupId,
    CAST(CASE WHEN AssessmentType = 2 THEN 21 END AS STRING) AS AssessmentType,
    CAST(NULLIF(TotalEnrolled, '*') AS INT64) AS TotalEnrolled,
    CAST(NULLIF(TotalTested, '*') AS INT64) AS TotalTested,
    CAST(NULLIF(TotalTestedWithScores, '*') AS INT64) AS TotalTestedWithScores,
    CASE WHEN Grade = 'KN' THEN 0 ELSE CAST(Grade AS INT64) END GradeLevel,
    CAST(NULLIF(OverallMeanSclScr, '*') AS FLOAT64) AS OverallMeanSclScr,
    ROUND(CAST(NULLIF(OverallPerfLvl1Pcnt, '*') AS FLOAT64)/100, 4) AS OverallPerfLvl1Pcnt,
    CAST(NULLIF(OverallPerfLvl1Count, '*') AS INT64) AS OverallPerfLvl1Count,
    ROUND(CAST(NULLIF(OverallPerfLvl2Pcnt, '*') AS FLOAT64)/100, 4) AS OverallPerfLvl2Pcnt,
    CAST(NULLIF(OverallPerfLvl2Count, '*') AS INT64) AS OverallPerfLvl2Count,
    ROUND(CAST(NULLIF(OverallPerfLvl3Pcnt, '*') AS FLOAT64)/100, 4) AS OverallPerfLvl3Pcnt,
    CAST(NULLIF(OverallPerfLvl3Count, '*') AS INT64) AS OverallPerfLvl3Count,
    ROUND(CAST(NULLIF(OverallPerfLvl4Pcnt, '*') AS FLOAT64)/100, 4) AS OverallPerfLvl4Pcnt,
    CAST(NULLIF(OverallPerfLvl4Count, '*') AS INT64) AS OverallPerfLvl4Count,
    CAST(NULLIF(OverallTotal, '*') AS INT64) AS OverallTotal,
    CAST(NULLIF(OralLangMeanSclScr, '*') AS FLOAT64) AS OralLangMeanSclScr,
    ROUND(CAST(NULLIF(OralLangPerfLvl1Pcnt, '*') AS FLOAT64)/100, 4) AS OralLangPerfLvl1Pcnt,
    CAST(NULLIF(OralLangPerfLvl1Count, '*') AS INT64) AS OralLangPerfLvl1Count,
    ROUND(CAST(NULLIF(OralLangPerfLvl2Pcnt, '*') AS FLOAT64)/100, 4) AS OralLangPerfLvl2Pcnt,
    CAST(NULLIF(OralLangPerfLvl2Count, '*') AS INT64) AS OralLangPerfLvl2Count,
    ROUND(CAST(NULLIF(OralLangPerfLvl3Pcnt, '*') AS FLOAT64)/100, 4) AS OralLangPerfLvl3Pcnt,
    CAST(NULLIF(OralLangPerfLvl3Count, '*') AS INT64) AS OralLangPerfLvl3Count,
    ROUND(CAST(NULLIF(OralLangPerfLvl4Pcnt, '*') AS FLOAT64)/100, 4) AS OralLangPerfLvl4Pcnt,
    CAST(NULLIF(OralLangPerfLvl4Count, '*') AS INT64) AS OralLangPerfLvl4Count,
    CAST(NULLIF(OralLangTotal, '*') AS INT64) AS OralLangTotal,
    CAST(NULLIF(WritLangMeanSclScr, '*') AS FLOAT64) AS WritLangMeanSclScr,
    ROUND(CAST(NULLIF(WritLangPerfLvl1Pcnt, '*') AS FLOAT64)/100, 4) AS WritLangPerfLvl1Pcnt,
    CAST(NULLIF(WritLangPerfLvl1Count, '*') AS INT64) AS WritLangPerfLvl1Count,
    ROUND(CAST(NULLIF(WritLangPerfLvl2Pcnt, '*') AS FLOAT64)/100, 4) AS WritLangPerfLvl2Pcnt,
    CAST(NULLIF(WritLangPerfLvl2Count, '*') AS INT64) AS WritLangPerfLvl2Count,
    ROUND(CAST(NULLIF(WritLangPerfLvl3Pcnt, '*') AS FLOAT64)/100, 4) AS WritLangPerfLvl3Pcnt,
    CAST(NULLIF(WritLangPerfLvl3Count, '*') AS INT64) AS WritLangPerfLvl3Count,
    ROUND(CAST(NULLIF(WritLangPerfLvl4Pcnt, '*') AS FLOAT64)/100, 4) AS WritLangPerfLvl4Pcnt,
    CAST(NULLIF(WritLangPerfLvl4Count, '*') AS INT64) AS WritLangPerfLvl4Count,
    CAST(NULLIF(WritLangTotal, '*') AS INT64) AS WritLangTotal,
    ROUND(CAST(NULLIF(ListeningDomainBeginPcnt, '*') AS FLOAT64)/100, 4) AS ListeningDomainBeginPcnt,
    CAST(NULLIF(ListeningDomainBeginCount, '*') AS INT64) AS ListeningDomainBeginCount,
    ROUND(CAST(NULLIF(ListeningDomainModeratePcnt, '*') AS FLOAT64)/100, 4) AS ListeningDomainModeratePcnt,
    CAST(NULLIF(ListeningDomainModerateCount, '*') AS INT64) AS ListeningDomainModerateCount,
    ROUND(CAST(NULLIF(ListeningDomainDevelopedPcnt, '*') AS FLOAT64)/100, 4) AS ListeningDomainDevelopedPcnt,
    CAST(NULLIF(ListeningDomainDevelopedCount, '*') AS INT64) AS ListeningDomainDevelopedCount,
    CAST(NULLIF(ListeningDomainTotal, '*') AS INT64) AS ListeningDomainTotal,
    ROUND(CAST(NULLIF(SpeakingDomainBeginPcnt, '*') AS FLOAT64)/100, 4) AS SpeakingDomainBeginPcnt,
    CAST(NULLIF(SpeakingDomainBeginCount, '*') AS INT64) AS SpeakingDomainBeginCount,
    ROUND(CAST(NULLIF(SpeakingDomainModeratePcnt, '*') AS FLOAT64)/100, 4) AS SpeakingDomainModeratePcnt,
    CAST(NULLIF(SpeakingDomainModerateCount, '*') AS INT64) AS SpeakingDomainModerateCount,
    ROUND(CAST(NULLIF(SpeakingDomainDevelopedPcnt, '*') AS FLOAT64)/100, 4) AS SpeakingDomainDevelopedPcnt,
    CAST(NULLIF(SpeakingDomainDevelopedCount, '*') AS INT64) AS SpeakingDomainDevelopedCount,
    CAST(NULLIF(SpeakingDomainTotal, '*') AS INT64) AS SpeakingDomainTotal,
    ROUND(CAST(NULLIF(ReadingDomainBeginPcnt, '*') AS FLOAT64)/100, 4) AS ReadingDomainBeginPcnt,
    CAST(NULLIF(ReadingDomainBeginCount, '*') AS INT64) AS ReadingDomainBeginCount,
    ROUND(CAST(NULLIF(ReadingDomainModeratePcnt, '*') AS FLOAT64)/100, 4) AS ReadingDomainModeratePcnt,
    CAST(NULLIF(ReadingDomainModerateCount, '*') AS INT64) AS ReadingDomainModerateCount,
    ROUND(CAST(NULLIF(ReadingDomainDevelopedPcnt, '*') AS FLOAT64)/100, 4) AS ReadingDomainDevelopedPcnt,
    CAST(NULLIF(ReadingDomainDevelopedCount, '*') AS INT64) AS ReadingDomainDevelopedCount,
    CAST(NULLIF(ReadingDomainTotal, '*') AS INT64) AS ReadingDomainTotal,
    ROUND(CAST(NULLIF(WritingDomainBeginPcnt, '*') AS FLOAT64)/100, 4) AS WritingDomainBeginPcnt,
    CAST(NULLIF(WritingDomainBeginCount, '*') AS INT64) AS WritingDomainBeginCount,
    ROUND(CAST(NULLIF(WritingDomainModeratePcnt, '*') AS FLOAT64)/100, 4) AS WritingDomainModeratePcnt,
    CAST(NULLIF(WritingDomainModerateCount, '*') AS INT64) AS WritingDomainModerateCount,
    ROUND(CAST(NULLIF(WritingDomainDevelopedPcnt, '*') AS FLOAT64)/100, 4) AS WritingDomainDevelopedPcnt,
    CAST(NULLIF(WritingDomainDevelopedCount, '*') AS INT64) AS WritingDomainDevelopedCount,
    CAST(NULLIF(WritingDomainTotal, '*') AS INT64) AS WritingDomainTotal
FROM {{ source('RawData', 'Elpac2019')}}
