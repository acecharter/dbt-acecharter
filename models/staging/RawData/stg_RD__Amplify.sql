with
students as (
    select *
    from {{ ref('dim_StudentDemographics') }}
),

amplify as (
    select
        '2022-23' as SchoolYear,
        '25' as AceAssessmentId,
        'Amplify ELA' as AceAssessmentName,
        'ELA' as Subject,
        case ELA_School_Name
            when 'ACE Empower Academy' then '116814'
            when 'ACE Esperanza Middle School' then '129247'
            when 'ACE Inspire Academy' then '131656'
        end as SchoolId,
        ELA_School_Name as SchoolName,
        ELA_Class_Name as ClassName,
        ELA_SIS_ID as ElaSisId,
        ELA_Last_Name as LastName,
        ELA_First_Name as FirstName,
        ELA_Email as Email,
        substr(ELA_Unit_Title, 1, 1) as GradeLevel,
        date(ELA_Hand_in_Date) as ElaHandInDate,
        ELA_Unit_Title as ElaUnitTitle,
        ELA_Lesson_Title as ElaLessonTitle,
        ELA_Test_Score_Achieved as ElaTestScoreAchieved,
        ELA_Test_Score_Potential as ElaTestScorePotential,
        ELA_Test_Score as ElaTestScore
    from {{ source('RawData', 'Amplify2223') }}
),

final as (
    select
        a.AceAssessmentId,
        a.AceAssessmentName,
        a.SchoolYear,
        a.Subject,
        a.SchoolId,
        a.SchoolName,
        a.ClassName,
        s.StudentUniqueId,
        s.StateUniqueId,
        a.LastName,
        a.FirstName,
        a.Email,
        a.GradeLevel,
        a.ElaHandInDate,
        a.ElaUnitTitle,
        a.ElaLessonTitle,
        a.ElaTestScoreAchieved,
        a.ElaTestScorePotential,
        a.ElaTestScore
    from amplify as a
    left join students as s
        on
            cast(a.ElaSisId as string) = s.SisUniqueId
            and a.SchoolYear = s.SchoolYear
)

select * from final
