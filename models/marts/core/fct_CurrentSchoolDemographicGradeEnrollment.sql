WITH current_students AS (
  SELECT *
  FROM {{ ref('dim_Students')}}
  WHERE IsCurrentlyEnrolled = TRUE
),

all_students AS (
  SELECT
    SchoolYear,
    SchoolId,
    'All Students' AS StudentGroupType,
    'All Students' AS StudentGroup,
    GradeLevel,
    COUNT(*) AS Enrollment
  FROM current_students
  GROUP BY 1, 2, 5
),

race_ethnicity AS(
  SELECT
    SchoolYear,
    SchoolId,
    'Race/Ethnicity' AS StudentGroupType,
    RaceEthnicity AS StudentGroup,
    GradeLevel,
    COUNT(*) AS Enrollment
  FROM current_students
  GROUP BY 1, 2, 4, 5
  ORDER BY 1, 2, 4, 5
),

el_status AS (
  SELECT
    SchoolYear,
    SchoolId,
    'English Learner Status' AS StudentGroupType,
    CASE 
        WHEN IsEll='Yes' THEN 'English Learner'
        WHEN IsEll='No' THEN 'Not English Learner'
    END AS StudentGroup,
    GradeLevel,
    COUNT(*) AS Enrollment
  FROM current_students
  GROUP BY 1, 2, 4, 5
  ORDER BY 1, 2, 4, 5
),

frl_status AS (
  SELECT
    SchoolYear,
    SchoolId,
    'Free/Reduced Meal Eligibility Status' AS StudentGroupType,
    CASE 
        WHEN HasFrl='Yes' THEN 'Free/Reduced Meal-Eligible'
        WHEN HasFrl='No' THEN 'Not Free/Reduced Meal-Eligible'
    END AS StudentGroup,
    GradeLevel,
    COUNT(*) AS Enrollment
  FROM current_students
  GROUP BY 1, 2, 4, 5
  ORDER BY 1, 2, 4, 5
),

sped_status AS (
  SELECT
    SchoolYear,
    SchoolId,
    'Special Education Status' AS StudentGroupType,
    CASE 
        WHEN HasIep='Yes' THEN 'Special Education'
        WHEN HasIep='No' THEN 'Not Special Education'
    END AS StudentGroup,
    GradeLevel,
    COUNT(*) AS Enrollment
  FROM current_students
  GROUP BY 1, 2, 4, 5
  ORDER BY 1, 2, 4, 5
),

gender AS (
  SELECT
    SchoolYear,
    SchoolId,
    'Gender' AS StudentGroupType,
    Gender AS StudentGroup,
    GradeLevel,
    COUNT(*) AS Enrollment
  FROM current_students
  GROUP BY 1, 2, 4, 5
  ORDER BY 1, 2, 4, 5
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
 
