select * from {{ ref('base_RD__Calpads217Elas2022') }}
union all
select * from {{ ref('base_RD__Calpads217Elas2023') }}
