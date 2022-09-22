SELECT *
FROM {{ ref('dim_Students')}}
WHERE IsCurrentlyEnrolled = TRUE