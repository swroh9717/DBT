{{config (
    materialized = 'table'
    ,enabled = true
)
}}

SELECT sd_item.sales_document
       ,sd_item.sales_document_item
       ,sd_item.material as material_no
       ,sd_item.description
       ,sd_item.item_category
       ,sd_item.reason_for_rejection
       ,sd_item.plant
       ,sd_item.storage_location
       ,sd_item.created_on
       ,sd_item.subtotal_2
       ,sd_item.cumulative_order_quantity
       ,sd_item.cumulative_confirmed_quantity_in_sales_unit
       ,sd_item.base_unit_of_measure
       ,sd_item.higher_level_item
       ,sd_item.bom_category
       ,sd_item.substitution_reason
       ,sd_item.batch as batch_no
       ,sd_item.sales_unit
       ,sd_item.target_quantity
       ,sd_item.document_currency
       ,sd_item.target_quantity_uom

       ,sd_item_status.delivery_status
       ,sd_item_status.billing_status
       ,sd_item_status.confirmed
       FROM {{ ref('sales_document_item_data_ecc') }} sd_item
       INNER JOIN {{ ref('sales_document_item_status_ecc') }} AS sd_item_status
        ON sd_item.sales_document = sd_item_status.sales_document
        AND sd_item.sales_document_item = sd_item_status.item_sd
       WHERE sd_item.client = '300'
  