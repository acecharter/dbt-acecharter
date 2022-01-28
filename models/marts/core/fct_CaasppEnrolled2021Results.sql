SELECT * EXCEPT (
  Attemptedness,
  ScoreStatus,
  IncludeIndicator
)
FROM {{ ref('stg_RawData__TomsCaasppEnrolled2021') }}
WHERE IncludeIndicator = 'Y'