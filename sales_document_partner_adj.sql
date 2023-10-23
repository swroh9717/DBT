{{config (
    materialized = 'table'
    ,enabled = true
)
}}

SELECT
*,
case when partner_function in ('AG','WE','RE','RG','SP','Z6','Z7','Z8','ZB','ZC','ZY') then customer
     when partner_function in ('P6','P7','P8','VE') then personnel_number
     when partner_function = 'LF' then vendor 
     end as partner_number

FROM {{ ref('sales_document_partner_ecc') }}
WHERE item_sd is null 