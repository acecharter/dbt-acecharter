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
      '2022 ELA SBAC' as AssessmentName,
      1 as AssessmentOrder,
      SbacElaDfs22 as StudentResult
    from dfs
  ),

  ela_q1 as (
    select
      StudentUniqueId,
      'ELA Q1 Benchmark' as AssessmentName,
      2 as AssessmentOrder,
      ElaDfsQ1 as StudentResult
    from dfs
  ),

  ela_q2 as (
    select
      StudentUniqueId,
      'ELA Q2 Benchmark' as AssessmentName,
      3 as AssessmentOrder,
      ElaDfsQ2 as StudentResult
    from dfs
  ),

  math_22 as (
    select
      StudentUniqueId,
      '2022Math SBAC' as AssessmentName,
      1 as AssessmentOrder,
      SbacMathDfs22 as StudentResult
    from dfs
  ),

  math_q1 as (
    select
      StudentUniqueId,
      'Math Q1 Benchmark' as AssessmentName,
      2 as AssessmentOrder,
      MathDfsQ1 as StudentResult
    from dfs
  ),

  math_q2 as (
    select
      StudentUniqueId,
      'Math Q2 Benchmark' as AssessmentName,
      3 as AssessmentOrder,
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