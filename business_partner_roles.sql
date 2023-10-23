{{config (
    materialized = 'table'
    ,enabled = true
)
}}

with pivot as (
select
  business_partner_1
  , {{ dbt_utils.pivot('relationship_cat_adj'
     , dbt_utils.get_column_values(ref('bp_relation_role_defs_gen_data_crm_adj'), 'relationship_cat_adj')
     , agg='max' 
     , then_value = 'business_partner_2'
     , else_value = '""') }}
from {{ ref('bp_relation_role_defs_gen_data_crm_adj') }}
where relationship_cat_adj in ('BUR011','BUR001_X','ZKAM')
and PARSE_DATE('%Y%m%d', valid_to) >= CURRENT_DATE('America/New_York')
group by business_partner_1
)

Select 
business_partner_1
,BUR011 as employee_responsible
,BUR001_X as primary_contact
,ZKAM as key_account_manager
from pivot