with
  current_esperanza_students as (
    select * except (ExitWithdrawDate, ExitWithdrawReason)
    from {{ ref('dim_Students')}}
    where IsCurrentlyEnrolled = TRUE
    and SchoolId = '129247'
  ),

  assessments as (
    select *
    from {{ ref('fct_StudentAssessment')}}
    where AssessmentObjective = 'Overall'
  ),

  dfs as (
    select * from {{ ref('stg_GSD__EsperanzaStudentConferenceData')}}
  ),

  ela_22 as (
    select
      StudentUniqueId,
      '2021-22' as AssessmentSchoolYear,
      'ELA' as AssessmentSubject,
      '2022 SBAC' as AssessmentPeriod,
      SbacElaDfs22 as StudentResult
    from dfs
  ),

  ela_q1 as (
    select
      StudentUniqueId,
      '2022-23' as AssessmentSchoolYear,
      'ELA' as AssessmentSubject,
      'Q1' as AssessmentPeriod,
      ElaDfsQ1 as StudentResult
    from dfs
  ),

  ela_q2 as (
    select
      StudentUniqueId,
      '2022-23' as AssessmentSchoolYear,
      'ELA' as AssessmentSubject,
      'Q2' as AssessmentPeriod,
      ElaDfsQ2 as StudentResult
    from dfs
  ),

  math_22 as (
    select
      StudentUniqueId,
      '2021-22' as AssessmentSchoolYear,
      'Math' as AssessmentSubject,
      '2022 SBAC' as AssessmentPeriod,
      SbacMathDfs22 as StudentResult
    from dfs
  ),

  math_q1 as (
    select
      StudentUniqueId,
      '2022-23' as AssessmentSchoolYear,
      'Math' as AssessmentSubject,
      'Q1' as AssessmentPeriod,
      MathDfsQ1 as StudentResult
    from dfs
  ),

  math_q2 as (
    select
      StudentUniqueId,
      '2022-23' as AssessmentSchoolYear,
      'Math' as AssessmentSubject,
      'Q2' as AssessmentPeriod,
      MathDfsQ2 as StudentResult
    from dfs
  ),

  unioned as (
    select * from ela_22
    union all
    select * from ela_q1
    union all
    select * from ela_q2
    union all
    select * from math_22
    union all
    select * from math_q1
    union all
    select * from math_q2
  ),

  final as (
    select
      s.*,
      u.* EXCEPT(StudentUniqueId)
    from current_esperanza_students as s
    left join unioned as u
    on s.StudentUniqueId = u.StudentUniqueId
  )

select * from final order by StateUniqueId