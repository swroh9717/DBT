{{config (
    materialized = 'table'
    ,enabled = true
)
}}


with pivot as (
  select
  business_partner
 , {{ dbt_utils.pivot('identification_type'
     , dbt_utils.get_column_values(ref('bp_id_numbers_s4'), 'identification_type')
     , agg='max' 
     , then_value = 'id_number'
     , else_value = '""') }}
from {{ ref('bp_id_numbers_s4') }}
where identification_type in ('BUP001','ZSYS','ZCX2G','ZCSM','ZCDMS','HCM001','ZLEASE')
group by business_partner

)

Select 
business_partner
,BUP001 as duns_no
,ZSYS as primary_billing_system
,ZCX2G as cx2_cust_guid
,ZCSM as customer_service_mgmt
,ZCDMS as cdm_status
,HCM001 as employee_no
,ZLEASE as lease_partner_type
from pivot