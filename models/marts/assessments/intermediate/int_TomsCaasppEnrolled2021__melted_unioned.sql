SELECT * FROM {{ ref('int_TomsCaasppEnrolled2021__melted_2021') }}
UNION ALL
SELECT * FROM {{ ref('int_TomsCaasppEnrolled2021__melted_2019') }}