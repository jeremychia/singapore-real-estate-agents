with
    agents as (
        select
            agent_id,
            agent_registration_number,
            agent_name,
            agent_aliases,
            agent_registration_validity_from,
            agent_registration_validity_to,
            agent_photo_url,
            agent_mobile_number,
        from {{ ref("dim_agents") }}
    ),

    agents_agencies as (
        select
            agent_registration_number,
            array_agg(
                struct(
                    agency_name,
                    agency_license_number,
                    agent_licence_validity_from,
                    agent_licence_validity_to
                )
            ) as agent_licence_information,
        from {{ ref("dim_agents_agencies_scd") }}
        group by all
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
        from {{ ref("fact_property_transactions") }}
    ),

    summarise_transactions as (
        select
            agent_registration_number,
            sum(
                case
                    when hdb_or_private = 'hdb' and rental_or_resale = 'resale'
                    then 1
                    else 0
                end
            ) as count_hdb_resale,
            sum(
                case
                    when hdb_or_private = 'hdb' and rental_or_resale = 'rental'
                    then 1
                    else 0
                end
            ) as count_hdb_rental,
            sum(
                case
                    when hdb_or_private = 'private' and rental_or_resale = 'sale'
                    then 1
                    else 0
                end
            ) as count_private_sale,
            sum(
                case
                    when hdb_or_private = 'private' and rental_or_resale = 'rental'
                    then 1
                    else 0
                end
            ) as count_private_rental,
            sum(
                case when lower(client) = 'buyer' then 1 else 0 end
            ) as count_client_buyer,
            sum(
                case when lower(client) = 'seller' then 1 else 0 end
            ) as count_client_seller,
            sum(
                case when lower(client) = 'tenant' then 1 else 0 end
            ) as count_client_tenant,
            sum(
                case when lower(client) = 'landlord' then 1 else 0 end
            ) as count_client_landlord,
            min(transaction_date) as earliest_transaction_date,
            max(transaction_date) as latest_transaction_date,
        from transactions
        group by all
    ),

    joined as (
        select *
        from agents
        left join summarise_transactions using (agent_registration_number)
        left join agents_agencies using (agent_registration_number)
    )

select *
from joined
