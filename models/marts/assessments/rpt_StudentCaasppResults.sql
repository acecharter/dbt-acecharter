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
      AssessmentFamilyNameShort,
      SystemOrVendorName
    FROM {{ ref('stg_GoogleSheetData__Assessments')}}
),

caaspp_results AS (
    SELECT
      r.StateUniqueId,
      r.AssessmentSchoolYear,
      r.AceAssessmentId,
      r.AssessmentName,
      i.AssessmentFamilyNameShort AS AssessmentFamily,
      i.SystemOrVendorName,
      r.AssessmentId,
      r.AssessedGradeLevel,
      r.ReportingMethod,
      CAST(r.StudentResult AS INT64) AS StudentResult
    FROM assessment_results AS r
    LEFT JOIN assessment_info AS i
    USING(AceAssessmentId)
    WHERE SystemOrVendorName = 'CAASPP'

)


SELECT
  s.* EXCEPT (SchoolId),
  cs.* EXCEPT (
      ExitWithdrawDate,
      ExitWithdrawReason
    ),
  r.* EXCEPT (StateUniqueId)
FROM current_students AS cs
LEFT JOIN caaspp_results AS r
ON cs.StateUniqueId = r.StateUniqueId
LEFT JOIN schools AS s
ON cs.SchoolId = s.SchoolId
WHERE AceAssessmentId IS NOT NULL
