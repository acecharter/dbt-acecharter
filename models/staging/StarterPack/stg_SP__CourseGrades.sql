with source_table as (
    select *
    from {{ source('StarterPack', 'CourseGrades') }}
    where
        date(_PARTITIONTIME) = current_date('America/Los_Angeles') --Use this line in lieu of the subsequent line during the school year
        -- date(_PARTITIONTIME) = '2024-06-19' --Update the date and use this line in lieu of the preceding line to keep grades dashboard updated over the summer
        and LetterGradeEarned is not null
),

school_year as (
    select distinct SchoolYear
    from {{ ref('stg_SP__CalendarDates') }}
),

final as (
    select
        school_year.SchoolYear,
        source_table.*
    from source_table
    cross join school_year
)

select * from final
