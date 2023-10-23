{{config (
    materialized = 'table'
    ,enabled = true
)
}}


with pivot as (
  select
  sales_document
 , {{ dbt_utils.pivot('partner_function'
     , dbt_utils.get_column_values(ref('sales_document_partner_adj'), 'partner_function')
     , agg='max'
     , then_value = 'partner_number'
     , else_value = '""') }}
from {{ ref('sales_document_partner_adj') }}
where partner_function in ('AG','WE','RE','RG','SP','Z6','Z7','Z8','P6','P7','P8','VE','ZB','ZC','ZY','LF')
group by sales_document
)

Select 
sales_document
, AG as sold_to
, WE as ship_to
, RE as bill_to
, RG as payer
, SP as forwarding_agent
, Z6 as originating_dealer
, Z7 as ordering_dealer
, Z8 as servicing_dealer
, P6 as originating_rep
, P7 as ordering_rep
, P8 as servicing_rep
, VE as sales_emp
, ZB as supply_deliv
, ZC as interim_consignee
, ZY as maintenance_payer
, LF as vendor
from pivot