{{config (
    materialized = 'table'
    ,enabled = true
)
}}


with pivot as (
  select
  business_partner
 , {{ dbt_utils.pivot('industry_system'
     , dbt_utils.get_column_values(ref('bp_industries_s4'), 'industry_system')
     , agg='max' 
     , prefix='_'
     , then_value = 'industry'
     , else_value = '""') }}
from {{ ref('bp_industries_s4') }}
where industry_system in ('ZIN1','ZIN2','ZIN3','ZIN4','SAPS','ZESC','ZIND','ZSPS','0003')
group by business_partner

)

Select 
business_partner
,_ZIN1 as ind_code_1
,_ZIN2 as ind_code_2
,_ZIN3 as ind_code_3
,_ZIN4 as ind_code_4
,_SAPS as sic_code
,_ZESC as sic_code_8digits
,_ZIND as industry
,_ZSPS as km_sic_code
,_0003 as naics_code
from pivot