{{ config(
    materialized='table'
) }}

with unpivoted as (
    {{ dbt_utils.unpivot(
        relation=ref('int_Cers__1_elpi_dfs_added'),
        cast_to='STRING',
        exclude=[
            'AssessmentId',
            'AceAssessmentId',
            'AceAssessmentName',
            'AceAssessmentSubject'
            'AssessmentName',
            'TestSchoolCdsCode',
            'StateUniqueId',
            'TestSchoolYear',
            'AssessmentDate',
            'Subject',
            'GradeLevelWhenAssessed',
            'AdministrationCondition'
        ],
        remove=[
            'TestDistrictId',
            'TestDistrictName',
            'TestSchoolName',
            'FirstName',
            'LastSurname',
            'TestSessionId',
            'AssessmentType',
            'AssessmentSubType',
            'Completeness'
        ],
        field_name='ReportingMethod',
        value_name='StudentResult'
    ) }}
),

final as (
    select
        AssessmentId,
        AceAssessmentId,
        case
            when
                AceAssessmentId = '15'
                then
                    concat(
                        'SB ELA',
                        regexp_extract(AssessmentName, '.+(\\s\\-\\s.+)')
                    )
            when
                AceAssessmentId = '16'
                then
                    concat(
                        'SB Math',
                        regexp_extract(AssessmentName, '.+(\\s\\-\\s.+)')
                    )
            else AceAssessmentName
        end as AssessmentName,
        AceAssessmentSubject as AssessmentSubject,
        cast(
            cast(
                right(TestSchoolCdsCode, 7)
                as int64
            )
            as string
        ) as TestedSchoolId,
        StateUniqueId,
        AssessmentDate,
        concat(
            cast(cast(TestSchoolYear as int64) - 1 as string),
            '-',
            right(cast(TestSchoolYear as string), 2)
        ) as SchoolYear,
        GradeLevelWhenAssessed,
        case
            when
                starts_with(AssessmentName, 'High')
                or starts_with(AssessmentName, 'Grade H')
                then 'High School'
            when
                starts_with(AssessmentName, 'Grade')
                then regexp_extract(AssessmentName, 'Grade (\\S+)')
            else AssessmentName
        end as AssessmentGradeLevel,
        AdministrationCondition,
        case
            when
                ReportingMethod in (
                    'ScaleScoreAchievementLevel',
                    'ScaleScore',
                    'DistanceFromStandard',
                    'ElpiLevel'
                )
                then 'Overall'
            when Subject = 'ELPAC'
                then (
                    case
                        when
                            starts_with(ReportingMethod, 'Alt1')
                            then 'Oral Language'
                        when
                            starts_with(ReportingMethod, 'Alt2')
                            then 'Written Language'
                        when
                            starts_with(ReportingMethod, 'Claim1')
                            then 'Listening'
                        when
                            starts_with(ReportingMethod, 'Claim2')
                            then 'Speaking'
                        when
                            starts_with(ReportingMethod, 'Claim3')
                            then 'Reading'
                        when
                            starts_with(ReportingMethod, 'Claim4')
                            then 'Writing'
                    end
                )
            when Subject = 'ELA'
                then (
                    case
                        when
                            starts_with(ReportingMethod, 'Claim1')
                            then 'Reading'
                        when
                            starts_with(ReportingMethod, 'Claim2')
                            then 'Writing'
                        when
                            starts_with(ReportingMethod, 'Claim3')
                            then 'Listening'
                        when
                            starts_with(ReportingMethod, 'Claim4')
                            then 'Research and Inquiry'
                    end
                )
            when Subject = 'Math'
                then (
                    case
                        when
                            starts_with(ReportingMethod, 'Claim1')
                            then 'Concepts and Procedures'
                        when
                            starts_with(ReportingMethod, 'Claim2')
                            then 'Problem Solving and Modeling & Data Analysis'
                        when
                            starts_with(ReportingMethod, 'Claim3')
                            then 'Communicating Reasoning'
                    end
                )
            when Subject = 'CAST' then (
                case
                    when
                        starts_with(ReportingMethod, 'Claim1')
                        then 'Life Sciences'
                    when
                        starts_with(ReportingMethod, 'Claim2')
                        then 'Physical Sciences'
                    when
                        starts_with(ReportingMethod, 'Claim3')
                        then 'Earth and Space Sciences'
                end
            )
        end as AssessmentObjective,
        case
            when
                ends_with(ReportingMethod, 'AchievementLevel')
                then 'Achievement Level'
            when ReportingMethod = 'ScaleScore' then 'Scale Score'
            when ReportingMethod = 'ElpiLevel' then 'ELPI Level'
            when
                ReportingMethod = 'DistanceFromStandard'
                then 'Distance From Standard'
        end as ReportingMethod,
        StudentResult,
        'INT64' as StudentResultDataType
    from unpivoted
    where
        StudentResult is not null
        and StudentResult != ''
)

select * from final
