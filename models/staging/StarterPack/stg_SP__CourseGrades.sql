with source_table as (
    select *
    from {{ source('StarterPack', 'CourseGrades') }}
    where
        date(_PARTITIONTIME) = current_date('America/Los_Angeles')
        --date(_PARTITIONTIME) = '2022-06-15' --Update the date and use this line in lieu of the preceding line to keep grades dashboard updated over the summer
        and LetterGradeEarned is not null
),

sy as (
    select * from {{ ref('dim_CurrentSchoolYear') }}
),

final as (
    select
        sy.SchoolYear,
        source_table.*
    from source_table
    cross join sy
)

select * from final
