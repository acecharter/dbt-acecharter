WITH 
  schools AS (
    SELECT
      SchoolId,
      SchoolName,
      SchoolNameMid,
      SchoolNameShort
    FROM {{ ref('dim_Schools')}}
  ),

  caaspp AS (
    SELECT
      CAST(CAST(SchoolCode AS INT64) AS STRING) AS SchoolId,
      TestYear,
      DemographicId,
      StudentGroup,
      DemographicName,
      GradeLevel,
      TestId,
      TestSubject,
      StudentsEnrolled,
      StudentsWithScores,
      PctStudentsWithScores,
      MeanScaleScore,
      StudentsStandardMetAndAbove,
      PctStandardMetAndAbove      
    FROM {{ ref('int_Caaspp__unioned_filtered_joined')}} 
  ),

  final AS (
    SELECT
      s.*,
      c.* EXCEPT (SchoolId)
    FROM schools AS s
    LEFT JOIN caaspp AS c
    USING (SchoolId)
  )

SELECT * FROM final
