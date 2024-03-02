with entity_caaspp as (
    select * from {{ ref('fct_EntityCaaspp') }}
),

recent_rfep as (
    select * from {{ ref('int_TomsCaasppTestedResults__recent_rfep_aggregated')}}
),

final as (
    select * from entity_caaspp
    union all
    select * from recent_rfep
)

select * from final