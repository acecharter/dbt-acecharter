with source_table as (
    select * from {{ source('StarterPack', 'CourseEnrollments_v2') }}
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
