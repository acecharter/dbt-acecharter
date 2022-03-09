WITH star_math AS (
  SELECT
    '10' AS AceAssessmentId,
    StudentIdentifier AS StudentUniqueId,
    TestedSchoolId,
    AssessmentId,
    AceTestingWindowName,
    StarTestingWindow
  FROM {{ref('stg_RS__Math_v2')}}
),

star_reading AS (
  SELECT
    '11' AS AceAssessmentId,
    StudentIdentifier AS StudentUniqueId,
    TestedSchoolId,
    AssessmentId,
    AceTestingWindowName,
    StarTestingWindow
  FROM {{ ref('stg_RS__Reading_v2')}}
),

star AS(
  SELECT * FROM star_reading
  UNION ALL
  SELECT * FROM star_math
),

star_keys AS(
  SELECT
    AceAssessmentId,
    StudentUniqueId,
    TestedSchoolId,
    COUNT(*) AS Duplicated
  FROM star
  GROUP BY 1, 2, 3
),

ace_window AS(
  SELECT
    AceAssessmentId,
    StudentUniqueId,
    TestedSchoolId,
    'ACE Testing Window' AS TestingWindowType,
    AceTestingWindowName AS TestingWindow,
    COUNT(AssessmentId) AS AssessmentResultCount
  FROM star
  WHERE AceTestingWindowName IS NOT NULL
  GROUP BY 1, 2, 3, 4, 5
),

star_window AS(
  SELECT
    AceAssessmentId,
    StudentUniqueId,
    TestedSchoolId,
    'Star Testing Window' AS TestingWindowType,
    StarTestingWindow AS TestingWindow,
    COUNT(AssessmentId) AS AssessmentResultCount
  FROM star
  GROUP BY 1, 2, 3, 4, 5
),

window_result_counts AS (
  SELECT * FROM ace_window
  UNION ALL 
  SELECT * FROM star_window
),

final AS (
SELECT
  k.* EXCEPT (Duplicated),
  r.TestingWindowType,
  r.TestingWindow,
  r.AssessmentResultCount
FROM star_keys AS k
LEFT JOIN window_result_counts AS r
ON
  k.AceAssessmentId = r.AceAssessmentId AND
  k.TestedSchoolId = r.TestedSchoolId AND
  k.StudentUniqueId = r.StudentUniqueId
)

SELECT * FROM final