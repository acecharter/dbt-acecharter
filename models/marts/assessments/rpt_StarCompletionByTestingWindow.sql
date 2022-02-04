WITH students AS (
  SELECT * FROM {{ ref('dim_Students') }}
),

schools AS (
    SELECT
      SchoolId,
      SchoolName,
      SchoolNameMid,
      SchoolNameShort
    FROM {{ ref('dim_Schools')}}
),

star_reading AS (
  SELECT
    SchoolYear,
    AssessmentName,
    TestedSchoolId,
    StudentIdentifier,
    StateUniqueId,
    AssessmentId,
    AceTestingWindowName,
    StarTestingWindow,
    DetailedTestingWindow2122
  FROM {{ ref('stg_RenaissanceStar__Reading_v2') }}
),

star_math AS (
  SELECT
    SchoolYear,
    AssessmentName,
    TestedSchoolId,
    StudentIdentifier,
    StateUniqueId,
    AssessmentId,
    AceTestingWindowName,
    StarTestingWindow,
    DetailedTestingWindow2122
    
  FROM {{ ref('stg_RenaissanceStar__Math_v2') }}
),

star AS(
  SELECT * FROM star_reading
  UNION ALL
  SELECT * FROM star_math
),


star_keys AS(
  SELECT
    * EXCEPT(
        AceTestingWindowName,
        StarTestingWindow,
        DetailedTestingWindow2122
      )
  FROM star
),

ace_window AS(
  SELECT
    AssessmentId,
    'ACE Testing Window' AS TestingWindowType,
    AceTestingWindowName AS TestingWindow
  FROM star
  WHERE AceTestingWindowName IS NOT NULL
),

detailed_window AS(
  SELECT
    AssessmentId,
    'Detailed Testing Window' AS TestingWindowType,
    DetailedTestingWindow2122 AS TestingWindow
  FROM star
),

star_window AS(
  SELECT
    AssessmentId,
    'Star Testing Window' AS TestingWindowType,
    StarTestingWindow AS TestingWindow
  FROM star
),

unioned_windows AS(
  SELECT 
    k.*,
    aw.* EXCEPT(AssessmentId)
  FROM star_keys AS k
  RIGHT JOIN ace_window AS aw
  USING (AssessmentId)

  UNION ALL
  SELECT 
    k.*,
    dw.* EXCEPT(AssessmentId)
  FROM star_keys AS k
  RIGHT JOIN detailed_window AS dw
  USING (AssessmentId)  

  UNION ALL
  SELECT 
    k.*,
    sw.* EXCEPT(AssessmentId)
  FROM star_keys AS k
  RIGHT JOIN star_window AS sw
  USING (AssessmentId)
),

grouped AS(
  SELECT
    SchoolYear,
    AssessmentName,
    TestedSchoolId,
    StudentIdentifier,
    StateUniqueId,
    TestingWindowType,
    TestingWindow,
    CASE WHEN COUNT(AssessmentId) > 0 THEN 'Yes' ELSE 'No' END AS TestedDuringWindow
  FROM unioned_windows
  GROUP BY 1, 2, 3, 4, 5, 6, 7
)

SELECT
  sc.* EXCEPT (SchoolId),
  st.* EXCEPT (ExitWithdrawReason),
  g.* EXCEPT (StudentIdentifier, StateUniqueId)

FROM students AS st
LEFT JOIN schools AS sc
ON st.SchoolId = sc.SchoolId
LEFT JOIN grouped AS g
ON st.StateUniqueId = g.StateUniqueId

