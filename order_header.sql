{{config (
    materialized = 'table'
    ,enabled = true
)
}}

SELECT sd_header_data.client
       ,sd_header_data.sales_document
       ,sd_header_data.sales_organization
       ,sd_header_data.distribution_channel
       ,sd_header_data.division
       ,sd_header_data.sales_office
       ,sd_header_data.sales_group
       ,sd_header_data.sd_document_categ
       ,sd_header_data.order_reason
       ,sd_header_data.delivery_block
       ,sd_header_data.billing_block
       ,sd_header_data.net_value
       ,sd_header_data.sold_to_party
       ,sd_header_data.created_on
       ,sd_header_data.sales_document_type
       ,sd_header_data.shipping_conditions
       ,sd_header_data.created_by
       ,sd_header_data.requested_deliv_date
       ,sd_header_data.warranty
       ,sd_header_data.collective_number
       ,sd_header_data.reference_document
       ,sd_header_data.preceding_doc_categ
       ,sd_header_data.entry_group_1 
       ,sd_header_data.opportunity_id
       ,sd_header_data.snumber
       ,sd_header_data.complete_delivery
       ,sd_header_data.sap_master_agrmnt_no
       ,sd_header_data.kit_number
       ,sd_header_data.entry_time
       ,sd_header_data.purchase_order_no

       ,sd_header_status.overall_creditstatus
       ,sd_header_status.overall_dlv_status
       ,sd_header_status.billing_status

       ,sd_business_data.sales_district
       ,sd_business_data.incoterms
       ,sd_business_data.incoterms_part_2
       ,sd_business_data.terms_of_payment
       ,sd_business_data.your_reference

       , sd_partner_codes.sold_to
       , sd_partner_codes.ship_to
       , sd_partner_codes.bill_to
       , sd_partner_codes.payer
       , sd_partner_codes.forwarding_agent
       , sd_partner_codes.originating_dealer
       , sd_partner_codes.ordering_dealer
       , sd_partner_codes.servicing_dealer
       , sd_partner_codes.originating_rep
       , sd_partner_codes.ordering_rep
       , sd_partner_codes.servicing_rep
       , sd_partner_codes.sales_emp
       , sd_partner_codes.supply_deliv
       , sd_partner_codes.interim_consignee
       , sd_partner_codes.maintenance_payer
       , sd_partner_codes.vendor
       FROM {{ ref('sales_document_header_data_ecc') }} sd_header_data
       INNER JOIN {{ ref('sales_document_header_status_and_administrative_data_ecc') }} sd_header_status
        on sd_header_data.sales_document = sd_header_status.sales_document
       LEFT JOIN {{ ref('sales_document_business_data_ecc') }} sd_business_data
        on sd_header_data.sales_document = sd_business_data.sales_document
        and sd_business_data.item_sd is null
       LEFT JOIN {{ ref('sales_document_partner_codes') }} sd_partner_codes
        on sd_header_data.sales_document = sd_partner_codes.sales_document
       WHERE sd_header_data.client = '300'