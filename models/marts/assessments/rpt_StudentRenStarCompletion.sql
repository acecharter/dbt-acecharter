WITH star_completion AS (
  SELECT * FROM {{ ref('dim_StudentRenStarCompletion') }}
),

star_keys AS(
  SELECT * EXCEPT (
    EnrolledOnCensusDate,
    EnrolledOnJan15,
    EnrolledOnJune9,
    FallMathResultCount,
    WinterMathResultCount,
    SpringMathResultCount,
    MathTestedBothFallSpring,
    FallReadingResultCount,
    WinterReadingResultCount,
    SpringReadingResultCount,
    ReadingTestedBothFallSpring,
    IncludeInFallMathCompletionRate,
    IncludeInWinterMathCompletionRate,
    IncludeInSpringMathCompletionRate,
    IncludeInFallSpringMathCompletionRate,
    IncludeInFallReadingCompletionRate,
    IncludeInWinterReadingCompletionRate,
    IncludeInSpringReadingCompletionRate,
    IncludeInFallSpringReadingCompletionRate
  )
  FROM star_completion
),

fall_math AS (
  SELECT
    SchoolId,
    StudentUniqueId,
    '10' AS AceAssessmentId,
    'Fall' AS TestingWindow,
    EnrolledOnCensusDate AS TestingRequired,
    CASE WHEN FallMathResultCount > 0 THEN 'Yes' ELSE 'No' END AS Tested,
    IncludeInFallMathCompletionRate AS IncludeInCompletionRate
  FROM star_completion
),

winter_math AS (
  SELECT
    SchoolId,
    StudentUniqueId,
    '10' AS AceAssessmentId,
    'Winter' AS TestingWindow,
    EnrolledOnJan15 AS TestingRequired,
    CASE WHEN WinterMathResultCount > 0 THEN 'Yes' ELSE 'No' END AS Tested,
    IncludeInWinterMathCompletionRate AS IncludeInCompletionRate
  FROM star_completion
),

spring_math AS (
  SELECT
    SchoolId,
    StudentUniqueId,
    '10' AS AceAssessmentId,
    'Spring' AS TestingWindow,
    EnrolledOnJune9 AS TestingRequired,
    CASE WHEN SpringMathResultCount > 0 THEN 'Yes' ELSE 'No' END AS Tested,
    IncludeInSpringMathCompletionRate AS IncludeInCompletionRate
  FROM star_completion
),

fall_reading AS (
  SELECT
    SchoolId,
    StudentUniqueId,
    '11' AS AceAssessmentId,
    'Fall' AS TestingWindow,
    EnrolledOnCensusDate AS TestingRequired,
    CASE WHEN FallMathResultCount > 0 THEN 'Yes' ELSE 'No' END AS Tested,
    IncludeInFallMathCompletionRate AS IncludeInCompletionRate
  FROM star_completion
),

winter_reading AS (
  SELECT
    SchoolId,
    StudentUniqueId,
    '11' AS AceAssessmentId,
    'Winter' AS TestingWindow,
    EnrolledOnJan15 AS TestingRequired,
    CASE WHEN WinterReadingResultCount > 0 THEN 'Yes' ELSE 'No' END AS Tested,
    IncludeInWinterReadingCompletionRate AS IncludeInCompletionRate
  FROM star_completion
),

spring_reading AS (
  SELECT
    SchoolId,
    StudentUniqueId,
    '11' AS AceAssessmentId,
    'Spring' AS TestingWindow,
    EnrolledOnJune9 AS TestingRequired,
    CASE WHEN SpringReadingResultCount > 0 THEN 'Yes' ELSE 'No' END AS Tested,
    IncludeInSpringReadingCompletionRate AS IncludeInCompletionRate
  FROM star_completion
),

fall_spring_math AS (
  SELECT
    SchoolId,
    StudentUniqueId,
    '10' AS AceAssessmentId,
    'Fall to Spring' AS TestingWindow,
    CASE WHEN EnrolledOnCensusDate = 'Yes' AND EnrolledOnJune9 = 'Yes' THEN 'Yes' ELSE 'No' END AS TestingRequired,
    CASE WHEN FallMathResultCount > 0 AND SpringMathResultCount > 0 THEN 'Yes' ELSE 'No' END AS Tested,
    IncludeInFallSpringMathCompletionRate AS IncludeInCompletionRate
  FROM star_completion
),

fall_spring_reading AS (
  SELECT
    SchoolId,
    StudentUniqueId,
    '11' AS AceAssessmentId,
    'Fall to Spring' AS TestingWindow,
    CASE WHEN EnrolledOnCensusDate = 'Yes' AND EnrolledOnJune9 = 'Yes' THEN 'Yes' ELSE 'No' END AS TestingRequired,
    CASE WHEN FallReadingResultCount > 0 AND SpringReadingResultCount > 0 THEN 'Yes' ELSE 'No' END AS Tested,
    IncludeInFallSpringReadingCompletionRate AS IncludeInCompletionRate
  FROM star_completion
),


completion_unioned AS(
  SELECT * FROM fall_math
  UNION ALL
  SELECT * FROM winter_math
  UNION ALL
  SELECT * FROM spring_math
  UNION ALL
  SELECT * FROM fall_spring_math
  UNION ALL
  SELECT * FROM fall_reading
  UNION ALL
  SELECT * FROM winter_reading
  UNION ALL
  SELECT * FROM spring_reading
  UNION ALL
  SELECT * FROM fall_spring_reading
)

SELECT
 s.*,
 c.* EXCEPT (SchoolId, StudentUniqueId)
FROM star_keys AS s
LEFT JOIN completion_unioned AS c
ON
  s.SchoolId = c.SchoolId
  AND s.StudentUniqueId = c.StudentUniqueId
WHERE IncludeInCompletionRate = 'Yes'
LIMIT 9909999999999