with results as (
    select * from {{ ref('base_RD__Ap2018')}}
    union all
    select * from {{ ref('base_RD__Ap2019')}}
    union all
    select * from {{ ref('base_RD__Ap2020')}}
    union all
    select * from {{ ref('base_RD__Ap2021')}}
    union all
    select * from {{ ref('base_RD__Ap2022')}}
    union all
    select * from {{ ref('base_RD__Ap2023')}}
),

ids as (
    select distinct
        ApId,
        StateUniqueId,
        StudentUniqueId
    from {{ ref('stg_GSD__ApStudentIds')}}
),

final as (
    select
        ids.StateUniqueId,
        ids.StudentUniqueId,
        results.*  
    from results
    left join ids
    using (ApId)
)

select * from final
