{{ config(
    materialized='table'
) }}

with unpivoted as (
    {{ dbt_utils.unpivot(
        relation=ref('int_Caaspp__1_filtered'),
        cast_to='STRING',
        exclude=[
            'AssessmentId',
            'AceAssessmentId',
            'AceAssessmentName',
            'AssessmentSubject',
            'EntityCode',
            'EntityType',
            'EntityName',
            'EntityNameMid',
            'EntityNameShort',
            'CountyCode',
            'DistrictCode',
            'SchoolCode',
            'SchoolYear',
            'TestYear',
            'TypeId',
            'DemographicId',
            'GradeLevel',
            'TestId',
            'StudentsEnrolled',
            'StudentsWithScores'
        ],
        remove=[
            'TestType',
            'TotalTestedWithScoresAtReportingLevel',
            'StudentsTested',
            'TypeId'
        ],
        field_name='ReportingMethod',
        value_name='SchoolResult'
    ) }}
),

final as (
    select
        * except (ReportingMethod),
        case
            when
                ReportingMethod like 'Mean%'
                or ReportingMethod like 'PctStandard%'
                then 'Overall'
            when
                AceAssessmentId = '1' and ReportingMethod like 'Area1%'
                then 'Reading'
            when
                AceAssessmentId = '1' and ReportingMethod like 'Area2%'
                then 'Writing'
            when
                AceAssessmentId = '1' and ReportingMethod like 'Area3%'
                then 'Listening'
            when
                AceAssessmentId = '1' and ReportingMethod like 'Area4%'
                then 'Research/Inquiry'
            when
                AceAssessmentId = '2' and ReportingMethod like 'Area1%'
                then 'Concepts & Procedures'
            when
                AceAssessmentId = '2' and ReportingMethod like 'Area2%'
                then 'Problem Solving and Modeling & Data Analysis'
            when
                AceAssessmentId = '2' and ReportingMethod like 'Area3%'
                then 'Communicating Reasoning'
        end as AssessmentObjective,
        case
            when ReportingMethod = 'MeanScaleScore' then 'Mean Scale Score'
            when
                ReportingMethod = 'MeanDistanceFromStandard'
                then 'Mean Distance From Standard'
            when
                ReportingMethod = 'PctStandardMetAndAbove'
                then 'Percent Met and Above'
            when ReportingMethod = 'PctStandardExceeded' then 'Percent Exceeded'
            when ReportingMethod = 'PctStandardMet' then 'Percent Met'
            when
                ReportingMethod = 'PctStandardNearlyMet'
                then 'Percent Nearly Met'
            when ReportingMethod = 'PctStandardNotMet' then 'Percent Not Met'
            when
                ReportingMethod like '%AboveStandard'
                then 'Percent Above Standard'
            when
                ReportingMethod like '%NearStandard'
                then 'Percent Near Standard'
            when
                ReportingMethod like '%BelowStandard'
                then 'Percent Below Standard'
        end as ReportingMethod,
        'FLOAT64' as ResultDataType,
        case
            when ReportingMethod like 'Mean%' then StudentsWithScores
            else round(StudentsWithScores * cast(SchoolResult as float64), 0)
        end as StudentWithResultCount
    from unpivoted
    where
        SchoolResult is not null
        and not (AceAssessmentId = '2' and ReportingMethod like 'Area4%')
)

select * from final
