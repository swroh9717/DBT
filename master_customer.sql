{{config (
    materialized = 'table'
    ,enabled = true
)
}}

with time_dependent_address_usages_s4 as (
  SELECT business_partner, address_number
  FROM {{ ref('time_dependent_address_usages_s4') }} 
  WHERE address_type = 'XXDEFAULT' and PARSE_DATETIME('%Y%m%d%H%M%S', CAST(valid_to as string)) >= CURRENT_DATETIME('America/New_York')
)

select s4gd.business_partner as customer_no
,abass4.company_name
,abass4.name_2
,abass4.name_3
,abass4.street_name_uppercase as street_name
,abass4.city_name_uppercase as city
,abass4.district
,abass4.postal_code
,abass4.region as state
,abass4.country_key as country
,abass4.search_term_1
,abass4.search_term_2
,abass4.telephone
,abass4.time_zone
,ins4p.duns_no
,crmgd.duns_parent_number
,crmgd.duns_global_number
,crmgd.annual_sales
,crmgd.number_of_employees
,crmgd.num_of_emp_at_site
,crmgd.account_type
,case when crmgd.active_account_2 ='I' then 'Inactive' 
      when crmgd.active_account_2 ='A' then 'Active' 
      else crmgd.active_account_2 end as customer_status
,crmgd.locationtype
,crmgd.duns_confidence_code
,crmgd.annual_sal_curr
,ics4p.ind_code_1
,ics4p.ind_code_2
,ics4p.ind_code_3
,ics4p.ind_code_4
,ics4p.industry
,ics4p.km_sic_code
,ics4p.sic_code_8digits
,ics4p.sic_code
,ics4p.naics_code
,ins4p.primary_billing_system
,ins4p.cx2_cust_guid
,ins4p.customer_service_mgmt
,ins4p.cdm_status
,ins4p.lease_partner_type
,rrdgdcrmp.employee_responsible
,rrdgdcrmp.primary_contact
,rrdgdcrmp.key_account_manager
,gdcms4.billableterms 
,case when gdcms4.retention_growth = '03' then 'Growth' 
      when gdcms4.retention_growth = '01' then 'Retention' 
      else gdcms4.retention_growth end as retention_growth
,case when s4gd.bp_type in ('0001','F001','Z001','Z006','ZCR1') then 'SOLD'
     when s4gd.bp_type in ('0002','F002','Z002','Z007','ZCR2') then 'SHIP'
     when s4gd.bp_type in ('0003','F004','Z003') then 'PAYR'
     when s4gd.bp_type in ('0004') then 'BILL'
     when s4gd.bp_type in ('Z005') or (s4gd.bp_type = '' and s4gd.business_partner_grouping = '0000') then 'PRPT'
     when s4gd.bp_type in ('ZSDA') then 'SUPP' 
     else '' end as customer_type

from {{ ref('bp_general_data_i_s4') }} s4gd
left join {{ ref('bp_general_data_i_crm') }} crmgd
  on s4gd.business_partner = crmgd.business_partner
left join time_dependent_address_usages_s4 as4
  on s4gd.business_partner = as4.business_partner
left join {{ ref('addresses_business_address_services_s4') }} abass4 
  on as4.address_number = abass4.address_number
left join {{ ref('business_partner_industry_codes') }} ics4p
  on s4gd.business_partner = ics4p.business_partner
left join {{ ref('business_partner_id_numbers') }} ins4p
  on s4gd.business_partner = ins4p.business_partner
left join {{ ref('business_partner_roles') }} rrdgdcrmp
  on s4gd.business_partner = rrdgdcrmp.business_partner_1
left join {{ ref('general_data_in_customer_master_s4') }} gdcms4
  on s4gd.business_partner = gdcms4.customer
where
(s4gd.bp_type in ('0001','F001','Z001','Z006','ZCR1','0002','F002','Z002','Z007','ZCR2','0003','F004','Z003','0004','Z005','ZSDA')
   or (s4gd.bp_type = '' and s4gd.business_partner_grouping = '0000'))
and PARSE_DATE('%Y%m%d', abass4.date_to) >= CURRENT_DATE('America/New_York')