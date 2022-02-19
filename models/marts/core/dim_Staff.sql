SELECT
  StaffUniqueId,
  StaffDisplayName
FROM {{ ref('stg_SP__CourseEnrollments_v2') }}
GROUP BY 1, 2
ORDER BY 2