WITH current_students AS (
  SELECT *
  FROM {{ ref('dim_Students') }}
  WHERE IsCurrentlyEnrolled = true
),

all_students AS (
  SELECT
    SchoolId,
    'All Students' AS StudentGroupType,
    'All Students' AS StudentGroup,
    GradeLevel,
    COUNT(*) AS Enrollment
  FROM current_students
  GROUP BY 1, 4
),

race_ethnicity AS(
  SELECT
    SchoolId,
    'Race/Ethnicity' AS StudentGroupType,
    RaceEthnicity AS StudentGroup,
    GradeLevel,
    COUNT(*) AS Enrollment
  FROM current_students
  GROUP BY 1, 3, 4
  ORDER BY 1, 3, 4
),

el_status AS (
  SELECT
    SchoolId,
    'English Learner Status' AS StudentGroupType,
    CASE 
        WHEN IsEll='Yes' THEN 'English Learner'
        WHEN IsEll='No' THEN 'Not English Learner'
    END AS StudentGroup,
    GradeLevel,
    COUNT(*) AS Enrollment
  FROM current_students
  GROUP BY 1, 3, 4
  ORDER BY 1, 3, 4
),

frl_status AS (
  SELECT
    SchoolId,
    'Free/Reduced Meal Eligibility Status' AS StudentGroupType,
    CASE 
        WHEN HasFrl='Yes' THEN 'Free/Reduced Meal-Eligible'
        WHEN HasFrl='No' THEN 'Not Free/Reduced Meal-Eligible'
    END AS StudentGroup,
    GradeLevel,
    COUNT(*) AS Enrollment
  FROM current_students
  GROUP BY 1, 3, 4
  ORDER BY 1, 3, 4
),

sped_status AS (
  SELECT
    SchoolId,
    'Special Education Status' AS StudentGroupType,
    CASE 
        WHEN HasIep='Yes' THEN 'Special Education'
        WHEN HasIep='No' THEN 'Not Special Education'
    END AS StudentGroup,
    GradeLevel,
    COUNT(*) AS Enrollment
  FROM current_students
  GROUP BY 1, 3, 4
  ORDER BY 1, 3, 4
),

gender AS (
  SELECT
    SchoolId,
    'Gender' AS StudentGroupType,
    Gender AS StudentGroup,
    GradeLevel,
    COUNT(*) AS Enrollment
  FROM current_students
  GROUP BY 1, 3, 4
  ORDER BY 1, 3, 4
),

final AS (
  SELECT * FROM all_students
  UNION ALL
  SELECT * FROM race_ethnicity
  UNION ALL
  SELECT * FROM el_status
  UNION ALL
  SELECT * FROM frl_status
  UNION ALL
  SELECT * FROM sped_status
  UNION ALL
  SELECT * FROM gender
)

SELECT * FROM final
 
