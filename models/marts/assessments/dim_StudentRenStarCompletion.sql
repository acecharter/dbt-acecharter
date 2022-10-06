WITH
  students AS (
    SELECT * FROM {{ ref('dim_Students') }}
  ),

  schools AS (
      SELECT
        SchoolYear,
        SchoolId,
        SchoolName,
        SchoolNameMid,
        SchoolNameShort
      FROM {{ ref('dim_Schools')}}
  ),

  star_results AS (
    SELECT
      SchoolYear,
      StarTestingWindow,
      TestedSchoolId AS SchoolId,
      StudentIdentifier AS StudentUniqueId,
      AceAssessmentId,
      COUNT(*) AS ResultCount
    FROM {{ref('stg_RenaissanceStar')}}
    GROUP BY 1, 2, 3, 4, 5
  ),

  fall_math AS (
    SELECT * FROM star_results
    WHERE
      AceAssessmentId = '10'
      AND StarTestingWindow = 'Fall' 
  ),

  winter_math AS (
    SELECT * FROM star_results
    WHERE
      AceAssessmentId = '10'
      AND StarTestingWindow = 'Winter' 
  ),

  spring_math AS (
    SELECT * FROM star_results
    WHERE
      AceAssessmentId = '10'
      AND StarTestingWindow = 'Spring' 
  ),

  fall_reading AS (
    SELECT * FROM star_results
    WHERE
      AceAssessmentId = '11'
      AND StarTestingWindow = 'Fall' 
  ),

  winter_reading AS (
    SELECT * FROM star_results
    WHERE
      AceAssessmentId = '11'
      AND StarTestingWindow = 'Winter' 
  ),

  spring_reading AS (
    SELECT * FROM star_results
    WHERE
      AceAssessmentId = '11'
      AND StarTestingWindow = 'Spring' 
  ),  

  joined AS (
    SELECT
      sc.*,
      st.* EXCEPT (SchoolId,SchoolYear),
      --Add cols indicating whether student was enrolled as of Fall, Winter, Spring dates
      CASE WHEN st.EntryDate <= '2021-10-06' AND st.ExitWithdrawDate > '2021-10-06' THEN 'Yes' ELSE 'No' END AS EnrolledOnCensusDate_21,
      CASE WHEN st.EntryDate <= '2022-01-15' AND st.ExitWithdrawDate > '2022-01-15' THEN 'Yes' ELSE 'No' END AS EnrolledOnJan15_22,
      CASE WHEN st.ExitWithdrawDate = '2022-06-10' THEN 'Yes' ELSE 'No' END AS EnrolledOnJune9_22,
      CASE WHEN fm.ResultCount > 0 THEN fm.ResultCount ELSE 0 END AS FallMathResultCount,
      CASE WHEN wm.ResultCount > 0 THEN wm.ResultCount ELSE 0 END AS WinterMathResultCount,
      CASE WHEN sm.ResultCount > 0 THEN sm.ResultCount ELSE 0 END AS SpringMathResultCount,
      CASE WHEN fm.ResultCount > 0 AND sm.ResultCount > 0 THEN 'Yes' ELSE 'No' END AS MathTestedBothFallSpring,
      CASE WHEN fr.ResultCount > 0 THEN fr.ResultCount ELSE 0 END AS FallReadingResultCount,
      CASE WHEN wr.ResultCount > 0 THEN wr.ResultCount ELSE 0 END AS WinterReadingResultCount,
      CASE WHEN sr.ResultCount > 0 THEN sr.ResultCount ELSE 0 END AS SpringReadingResultCount,
      CASE WHEN fr.ResultCount > 0 AND sr.ResultCount > 0 THEN 'Yes' ELSE 'No' END AS ReadingTestedBothFallSpring,
    FROM students AS st
    LEFT JOIN schools AS sc
    ON
      st.SchoolId = sc.SchoolId
      AND st.SchoolYear = sc.SchoolYear
    LEFT JOIN fall_math AS fm
    ON
      st.StudentUniqueId = fm.StudentUniqueId
      AND st.SchoolId = fm.SchoolId
      AND st.SchoolYear = fm.SchoolYear
    LEFT JOIN winter_math AS wm
    ON
      st.StudentUniqueId = wm.StudentUniqueId
      AND st.SchoolId = wm.SchoolId
      AND st.SchoolYear = wm.SchoolYear
    LEFT JOIN spring_math AS sm
    ON
      st.StudentUniqueId = sm.StudentUniqueId
      AND st.SchoolId = sm.SchoolId
      AND st.SchoolYear = sm.SchoolYear
    LEFT JOIN fall_reading AS fr
    ON
      st.StudentUniqueId = fr.StudentUniqueId
      AND st.SchoolId = fr.SchoolId
      AND st.SchoolYear = fr.SchoolYear
    LEFT JOIN winter_reading AS wr
    ON
      st.StudentUniqueId = wr.StudentUniqueId
      AND st.SchoolId = wr.SchoolId
      AND st.SchoolYear = wr.SchoolYear
    LEFT JOIN spring_reading AS sr
    ON
      st.StudentUniqueId = sr.StudentUniqueId
      AND st.SchoolId = sr.SchoolId
      AND st.SchoolYear = sr.SchoolYear
  ),

  final AS (
    SELECT
      *,
      CASE WHEN EnrolledOnCensusDate_21 = 'Yes' OR FallMathResultCount > 0 THEN 'Yes' ELSE 'No' END AS IncludeInFallMathCompletionRate,
      CASE WHEN EnrolledOnJan15_22 = 'Yes' OR WinterMathResultCount > 0 THEN 'Yes' ELSE 'No' END AS IncludeInWinterMathCompletionRate,
      CASE WHEN EnrolledOnJune9_22 = 'Yes' OR SpringMathResultCount > 0 THEN 'Yes' ELSE 'No' END AS IncludeInSpringMathCompletionRate,
      CASE
        WHEN
          (EnrolledOnCensusDate_21 = 'Yes' OR FallMathResultCount > 0)
          AND (EnrolledOnJune9_22 = 'Yes' OR SpringMathResultCount > 0)
        THEN 'Yes' ELSE 'No'
      END AS IncludeInFallSpringMathCompletionRate,
      CASE WHEN EnrolledOnCensusDate_21 = 'Yes' OR FallReadingResultCount > 0 THEN 'Yes' ELSE 'No' END AS IncludeInFallReadingCompletionRate,
      CASE WHEN EnrolledOnJan15_22 = 'Yes' OR WinterReadingResultCount > 0 THEN 'Yes' ELSE 'No' END AS IncludeInWinterReadingCompletionRate,
      CASE WHEN EnrolledOnJune9_22 = 'Yes' OR SpringReadingResultCount > 0 THEN 'Yes' ELSE 'No' END AS IncludeInSpringReadingCompletionRate,
      CASE
        WHEN
          (EnrolledOnCensusDate_21 = 'Yes' OR FallReadingResultCount > 0)
          AND (EnrolledOnJune9_22 = 'Yes' OR SpringReadingResultCount > 0)
        THEN 'Yes' ELSE 'No'
      END AS IncludeInFallSpringReadingCompletionRate,
    FROM joined
  )

SELECT *
FROM final
ORDER BY
  SchoolYear,
  SchoolId,
  StudentUniqueId



  