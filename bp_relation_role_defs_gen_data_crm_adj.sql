{{config (
    materialized = 'table'
    ,enabled = true
)
}}
 
 Select *,
    case when relationship_cat = 'BUR001' and standard_relationship_1 = 'X' then 'BUR001_X'
         else relationship_cat end as relationship_cat_adj
 from {{ ref('bp_relationships_role_definitions_general_data_crm') }}