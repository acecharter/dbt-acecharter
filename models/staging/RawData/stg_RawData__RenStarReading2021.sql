WITH assessment_ids AS (
  SELECT 
    AceAssessmentId,
    AssessmentNameShort AS AssessmentName,
    'Star Reading Enterprise Tests' AS AssessmentType
  FROM {{ ref('stg_GoogleSheetData__Assessments') }}
  WHERE AssessmentNameShort = 'Star Reading'
),

star_reading AS (
  SELECT
    Activity_Type AS AssessmentType,
    CASE
      WHEN School_Id='gs_4e804ecc-4623-46b4-a91a-fe2acb88cbb3' THEN '116814'
      WHEN School_Id='gs_e8341d4c-4366-43e1-99b5-71f66cec337a' THEN '129247'
      WHEN School_Id='gs_b3f0fcec-7391-479a-93fc-132cf73faa43' THEN '131656'
      WHEN School_Id='gs_4cfd50a2-23e1-4e6f-bb7d-e25ddee340a2' THEN '125617'
      ELSE '999999999'
    END AS TestedSchoolId,
    School_Name AS TestedSchoolName,
    CONCAT(LEFT(School_Year, 4), '-', RIGHT(School_Year, 2)) AS SchoolYear,
    CAST(NULL AS STRING) AS StudentRenaissanceID,
    CAST(NULL AS STRING) AS StudentIdentifier,
    Student_State_ID AS StateUniqueId,
    CASE
      WHEN Student_Middle_Name IS NULL THEN CONCAT(Student_Last_Name, ", ", Student_First_Name)
      ELSE CONCAT(Student_Last_Name, ", ", Student_First_Name, " ", Student_Middle_Name)
    END AS DisplayName,
    Student_Last_Name AS LastName,
    Student_First_Name,
    Student_Middle_Name,
    CAST(NULL AS STRING) AS Gender,
    DATE(Birthdate) AS BirthDate,
    Current_Grade As GradeLevel,
    CAST(NULL AS STRING) AS EnrollmentStatus,
    Renaissance_Activity_ID AS AssessmentId,
    DATE(Activity_Completed_Date) AS AssessmentDate,
    CAST(NULL AS INT64) AS AssessmentNumber,
    CAST(NULL AS FLOAT64) AS GradePlacement,
    Current_Grade AS Grade,
    Grade_Equivalent AS GradeEquivalent,
    Scaled_Score AS ScaledScore,
    Unified_Scale AS UnifiedScore,
    Percentile_Rank AS PercentileRank,
    Normal_Curve_Equivalent AS NormalCurveEquivalent,
    Instructional_Reading_Level AS InstructionalReadingLevel,
    Lexile_Score AS Lexile,
    CASE WHEN Current_SGP_Vector = 'FALL_FALL' THEN Current_SGP ELSE NULL END AS StudentGrowthPercentileFallFall,
    CASE WHEN Current_SGP_Vector = 'FALL_SPRING' THEN Current_SGP ELSE NULL END AS StudentGrowthPercentileFallSpring,
    CASE WHEN Current_SGP_Vector = 'FALL_WINTER' THEN Current_SGP ELSE NULL END AS StudentGrowthPercentileFallWinter,
    CASE WHEN Current_SGP_Vector = 'SPRING_SPRING' THEN Current_SGP ELSE NULL END AS StudentGrowthPercentileSpringSpring,
    CASE WHEN Current_SGP_Vector = 'WINTER_SPRING' THEN Current_SGP ELSE NULL END AS StudentGrowthPercentileWinterSpring,
    Current_SGP AS CurrentSGP,
    CAST(NULL AS STRING) AS AceTestingWindowName,
    CAST(NULL AS DATE) AS AceTestingWindowStartDate,
    CAST(NULL AS DATE) AS AceTestingWindowEndDate,
    CASE
      WHEN Activity_Completed_Date BETWEEN '2020-08-01' AND '2020-11-30'THEN 'Fall'
      WHEN Activity_Completed_Date BETWEEN '2020-12-01' AND '2021-03-31' THEN 'Winter'
      WHEN Activity_Completed_Date BETWEEN '2021-04-01' AND'2021-07-31' THEN 'Spring'
    END AS StarTestingWindow

  FROM {{ source('RawData', 'RenStarReading2021')}}
)

SELECT
  a.* EXCEPT(AssessmentType),
  s.* EXCEPT(AssessmentType)
FROM assessment_ids AS a
LEFT JOIN star_reading as s
USING (AssessmentType)