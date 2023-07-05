with final as (
    select * from {{ ref('base_RD__Anet2122')}}
    union all
    select * from {{ ref('base_RD__Anet2223')}}
)

select * from final
