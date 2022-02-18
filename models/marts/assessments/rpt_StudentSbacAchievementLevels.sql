WITH current_students AS (
    SELECT *
    FROM {{ ref('dim_Students') }}
    WHERE IsCurrentlyEnrolled = true
),

schools AS (
    SELECT
      SchoolId,
      SchoolName,
      SchoolNameMid,
      SchoolNameShort
    FROM {{ ref('dim_Schools')}}
),

assessment_results AS (
    SELECT *
    FROM {{ ref('fct_StudentAssessment')}}
),

assessment_info AS (
    SELECT 
      AceAssessmentId,
      AssessmentSubject,
      AssessmentFamilyNameShort,
      SystemOrVendorName
    FROM {{ ref('stg_GSD__Assessments')}}
),

sbac_results AS (
    SELECT
      r.StateUniqueId,
      r.AssessmentSchoolYear,
      r.AceAssessmentId,
      r.AssessmentName,
      i.AssessmentSubject,
      i.SystemOrVendorName,
      r.AssessmentId,
      r.AssessedGradeLevel,
      CAST(r.StudentResult AS INT64) AS AchievementLevel,
      CASE
        WHEN r.StudentResult = '1' THEN 'Not Met'
        WHEN r.StudentResult = '2' THEN 'Nearly Met'
        WHEN r.StudentResult = '3' THEN 'Met'
        WHEN r.StudentResult = '4' THEN 'Exceeded'
      END AS AchievementLevelCategory
    FROM assessment_results AS r
    LEFT JOIN assessment_info AS i
    USING(AceAssessmentId)
    WHERE 
      i.SystemOrVendorName = 'CAASPP' AND 
      i.AssessmentFamilyNameShort = 'SBAC' AND
      r.ReportingMethod = 'Achievement Level' AND
      CAST(r.StudentResult AS INT64) <= 4

)


SELECT
  s.* EXCEPT (SchoolId),
  cs.* EXCEPT (
      ExitWithdrawDate,
      ExitWithdrawReason
    ),
  r.* EXCEPT (StateUniqueId)
FROM current_students AS cs
LEFT JOIN sbac_results AS r
ON cs.StateUniqueId = r.StateUniqueId
LEFT JOIN schools AS s
ON cs.SchoolId = s.SchoolId
WHERE AceAssessmentId IS NOT NULL
