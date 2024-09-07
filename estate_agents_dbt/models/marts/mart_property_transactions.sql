with
    property_transactions as (
        select
            transaction_id,
            transaction_date,
            transaction_type,
            hdb_or_private,
            rental_or_resale,
            property_town,
            property_district_number,
            property_general_location,
            property_type,
            client,
            agent_registration_number
        from {{ ref("fact_property_transactions") }}
    ),

    agents as (
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

    districts as (
        select property_district_number, region, central_region_category
        from {{ ref("dim_districts") }}
    ),

    towns as (
        select property_town, region, central_region_category
        from {{ ref("dim_towns") }}
    ),

    joined as (
        select
            property_transactions.transaction_id,
            property_transactions.transaction_date,
            property_transactions.transaction_type,
            property_transactions.hdb_or_private,
            property_transactions.rental_or_resale,
            property_transactions.property_town,
            property_transactions.property_district_number,
            property_transactions.property_general_location,
            coalesce(
                cast(districts.region as string), cast(towns.region as string)
            ) as property_region,
            coalesce(
                cast(districts.central_region_category as string),
                cast(towns.central_region_category as string)
            ) as property_central_region_category,
            property_transactions.property_type,
            property_transactions.client,
            agents.agent_id,
            property_transactions.agent_registration_number,
            agents.agent_name,
            agents.agent_alias,
            agents.agent_registration_validity_from,
            agents.agent_registration_validity_to,
            agents.agent_photo_url,
            agents.agent_mobile_number,
            agents.agency_license_number,
            agents.agency_name
        from property_transactions
        left join
            agents
            on property_transactions.agent_registration_number
            = agents.agent_registration_number
        left join
            districts
            on coalesce(property_transactions.property_district_number, 0)
            = districts.property_district_number
        left join
            towns
            on coalesce(property_transactions.property_town, ' ') = towns.property_town
    )

select *
from joined
