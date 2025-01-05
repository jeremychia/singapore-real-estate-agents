with agents as (
    select
        agent_id,
        agent_registration_number,
        agent_name,
        agent_alias,
        agent_registration_validity_from,
        agent_registration_validity_to,
        agent_photo_url,
        agent_mobile_number,
        agency_license_number,
        agency_name
    from {{ ref("dim_agents") }}
),

transactions as (
    select
        transaction_id,
        transaction_date,
        transaction_type,
        hdb_or_private,
        rental_or_resale,
        client,
        agent_registration_number
    from {{ ref("fact_property_transactions" )}}
),

summarise_transactions as (
    
)

select *
from summarise_transactions
