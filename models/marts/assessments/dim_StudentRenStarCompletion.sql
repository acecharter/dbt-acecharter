WITH
  students AS (
    SELECT * FROM {{ ref('dim_Students') }}
  ),

  schools AS (
      SELECT
        SchoolId,
        SchoolName,
        SchoolNameMid,
        SchoolNameShort
      FROM {{ ref('dim_Schools')}}
  ),

  assessments AS (
    SELECT * FROM {{ ref('stg_GSD__Assessments')}}
  ),

  star_results AS (
    SELECT
      SchoolYear,
      StarTestingWindow,
      TestedSchoolId AS SchoolId,
      StudentIdentifier AS StudentUniqueId,
      AceAssessmentId,
      COUNT(*) AS ResultCount
    FROM {{ref('int_RenStar__1_unioned')}}
    WHERE SchoolYear = '2021-22'
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

  testing_windows AS (
    SELECT 
      * EXCEPT(EligibleStudentsEnrollmentDate),
      CASE
        WHEN EligibleStudentsEnrollmentDate > CURRENT_DATE() THEN CURRENT_DATE()
        ELSE EligibleStudentsEnrollmentDate
      END AS EligibleStudentsEnrollmentDate
    FROM {{ ref('stg_GSD__RenStarTestingWindows')}}
    WHERE TestingWindowStartDate < CURRENT_DATE()
  ),

--   student_window_combos AS (
--     SELECT
--         w.SchoolYear,
--         w.TestingWindow,
--         s.SchoolId,
--         s.StudentUniqueId
--     FROM students AS s
--     CROSS JOIN testing_windows AS w
--     WHERE
--         s.EntryDate <= w.EligibleStudentsEnrollmentDate AND
--         s.ExitWithdrawDate >= w.EligibleStudentsEnrollmentDate
--   ),

  

  joined AS (
    SELECT
      sc.*,
      st.* EXCEPT (SchoolId),
      --c.SchoolYear, --update this to calculate school year from entry date
      --Add cols indicating whether student was enrolled as of Fall, Winter, Spring dates
      CASE WHEN st.EntryDate <= '2021-10-06' AND st.ExitWithdrawDate > '2021-10-06' THEN 'Yes' ELSE 'No' END AS EnrolledOnCensusDate,
      CASE WHEN st.EntryDate <= '2022-01-15' AND st.ExitWithdrawDate > '2022-01-15' THEN 'Yes' ELSE 'No' END AS EnrolledOnJan15,
      CASE WHEN st.ExitWithdrawDate = '2022-06-10' THEN 'Yes' ELSE 'No' END AS EnrolledOnJune9,
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
    ON st.SchoolId = sc.SchoolId
    LEFT JOIN fall_math AS fm
    ON
      st.StudentUniqueId = fm.StudentUniqueId
      AND st.SchoolId = fm.SchoolId
    LEFT JOIN winter_math AS wm
    ON
      st.StudentUniqueId = wm.StudentUniqueId
      AND st.SchoolId = wm.SchoolId
    LEFT JOIN spring_math AS sm
    ON
      st.StudentUniqueId = sm.StudentUniqueId
      AND st.SchoolId = sm.SchoolId
    LEFT JOIN fall_reading AS fr
    ON
      st.StudentUniqueId = fr.StudentUniqueId
      AND st.SchoolId = fr.SchoolId
    LEFT JOIN winter_reading AS wr
    ON
      st.StudentUniqueId = wr.StudentUniqueId
      AND st.SchoolId = wr.SchoolId
    LEFT JOIN spring_reading AS sr
    ON
      st.StudentUniqueId = sr.StudentUniqueId
      AND st.SchoolId = sr.SchoolId
  ),

  final AS (
    SELECT
      *,
      CASE WHEN EnrolledOnCensusDate = 'Yes' OR FallMathResultCount > 0 THEN 'Yes' ELSE 'No' END AS IncludeInFallMathCompletionRate,
      CASE WHEN EnrolledOnJan15 = 'Yes' OR WinterMathResultCount > 0 THEN 'Yes' ELSE 'No' END AS IncludeInWinterMathCompletionRate,
      CASE WHEN EnrolledOnJune9 = 'Yes' OR SpringMathResultCount > 0 THEN 'Yes' ELSE 'No' END AS IncludeInSpringMathCompletionRate,
      CASE
        WHEN
          (EnrolledOnCensusDate = 'Yes' OR FallMathResultCount > 0)
          AND (EnrolledOnJune9 = 'Yes' OR SpringMathResultCount > 0)
        THEN 'Yes' ELSE 'No'
      END AS IncludeInFallSpringMathCompletionRate,
      CASE WHEN EnrolledOnCensusDate = 'Yes' OR FallReadingResultCount > 0 THEN 'Yes' ELSE 'No' END AS IncludeInFallReadingCompletionRate,
      CASE WHEN EnrolledOnJan15 = 'Yes' OR WinterReadingResultCount > 0 THEN 'Yes' ELSE 'No' END AS IncludeInWinterReadingCompletionRate,
      CASE WHEN EnrolledOnJune9 = 'Yes' OR SpringReadingResultCount > 0 THEN 'Yes' ELSE 'No' END AS IncludeInSpringReadingCompletionRate,
      CASE
        WHEN
          (EnrolledOnCensusDate = 'Yes' OR FallReadingResultCount > 0)
          AND (EnrolledOnJune9 = 'Yes' OR SpringReadingResultCount > 0)
        THEN 'Yes' ELSE 'No'
      END AS IncludeInFallSpringReadingCompletionRate,
    FROM joined
  )

SELECT *
FROM final
ORDER BY
  SchoolId,
  StudentUniqueId
LIMIT 10000000000
  --SchoolYear



  