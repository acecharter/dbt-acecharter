WITH student_demographics AS (
    SELECT * FROM {{ ref('stg_StarterPack__StudentDemographics')}}
),
students_with_iep AS (
    SELECT * FROM {{ ref('stg_RawData__Seis')}}
    WHERE StudentEligibilityStatus = 'Eligible/Previously Eligible'
)

SELECT
  sd.*,
  CASE
    WHEN i.StudentEligibilityStatus = 'Eligible/Previously Eligible' THEN TRUE
    ELSE FALSE
  END AS HasIep,
  i.StudentEligibilityStatus AS SeisEligibilityStatus,
  i.SeisExtractDate AS IepStatusDate 
FROM student_demographics AS sd
LEFT JOIN students_with_iep AS i
USING (SSID)