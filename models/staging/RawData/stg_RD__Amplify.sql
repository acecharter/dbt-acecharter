WITH
students AS (
    SELECT *
    FROM {{ ref('dim_StudentDemographics')}}
),

amplify AS (
    SELECT
        '2022-23' AS SchoolYear,
        '25' AS AceAssessmentId,
        'Amplify ELA' AS AceAssessmentName,
        'ELA' AS Subject,
        CASE ELA_School_Name
            WHEN 'ACE Empower Academy' THEN '116814'
            WHEN 'ACE Esperanza Middle School' THEN '129247'
            WHEN 'ACE Inspire Academy' THEN '131656'
        END AS SchoolId,
        ELA_School_Name AS SchoolName,
        ELA_Class_Name AS ClassName,
        ELA_SIS_ID AS ElaSisId,
        ELA_Last_Name AS LastName,
        ELA_First_Name AS FirstName,
        ELA_Email AS Email,
        SUBSTR(ELA_Unit_Title,1,1) AS GradeLevel,
        DATE(ELA_Hand_in_Date) AS ElaHandInDate,
        ELA_Unit_Title AS ElaUnitTitle,
        ELA_Lesson_Title AS ElaLessonTitle,
        ELA_Test_Score_Achieved AS ElaTestScoreAchieved,
        ELA_Test_Score_Potential AS ElaTestScorePotential,
        ELA_Test_Score AS ElaTestScore
    FROM {{ source('RawData', 'Amplify2223')}}
),

final AS (
    SELECT
        a.AceAssessmentId,
        a.AceAssessmentName,
        a.SchoolYear,
        a.Subject,
        a.SchoolId,
        a.SchoolName,
        a.ClassName,
        s.StudentUniqueId,
        s.StateUniqueId,
        a.LastName,
        a.FirstName,
        a.Email,
        a.GradeLevel,
        a.ElaHandInDate,
        a.ElaUnitTitle,
        a.ElaLessonTitle,
        a.ElaTestScoreAchieved,
        a.ElaTestScorePotential,
        a.ElaTestScore
    FROM amplify AS a
    LEFT JOIN students AS s
    ON
        CAST(a.ElaSisId AS STRING) = s.SisUniqueId
        AND a.SchoolYear = s.SchoolYear
)

SELECT * FROM final