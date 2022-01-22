WITH student_demographics AS (
    SELECT * FROM {{ ref('stg_StarterPack__StudentDemographics')}}
),
students_with_iep AS (
    SELECT * FROM {{ ref('stg_RawData__Seis')}}
    WHERE StudentEligibilityStatus = 'Eligible/Previously Eligible'
)

SELECT
 sd.*,
 CASE WHEN i.SeisUniqueId IS NOT NULL THEN TRUE ELSE FALSE END AS HasIep,
 i.SeisExtractDate AS IepStatusDate 
FROM student_demographics AS sd
LEFT JOIN students_with_iep AS i
USING (StateUniqueId)