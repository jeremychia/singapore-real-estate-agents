with
    property_transactions as (
        select
            property_transaction_key,
            transaction_id,
            transaction_date,
            transaction_month,
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
            agent_licence_validity_from,
            agent_licence_validity_to,
            agency_name,
            agency_license_number,
        from {{ ref("dim_agents_agencies_scd") }}
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
            property_transactions.property_transaction_key,
            property_transactions.transaction_id,
            property_transactions.transaction_date,
            property_transactions.transaction_month,
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
            agents.agent_aliases,
            agents.agent_registration_validity_from,
            agents.agent_registration_validity_to,
            agents.agent_photo_url,
            agents.agent_mobile_number,
            agents_agencies.agency_name,
            agents_agencies.agency_license_number,
        from property_transactions
        left join
            agents
            on property_transactions.agent_registration_number
            = agents.agent_registration_number
        left join
            agents_agencies
            on property_transactions.agent_registration_number
            = agents_agencies.agent_registration_number
            and property_transactions.transaction_date
            between agents_agencies.agent_licence_validity_from
            and agents_agencies.agent_licence_validity_to
        left join
            districts
            on coalesce(property_transactions.property_district_number, 0)
            = districts.property_district_number
        left join
            towns
            on coalesce(property_transactions.property_town, ' ') = towns.property_town
    ),

    -- using a backup logic to populate agency name for those missing
    non_null_agency_name as (select * from joined where agency_name is not null),

    null_agency_name as (select * from joined where agency_name is null),

    null_agency_name_get_nearest_agency_match as (
        select
            null_agency_name.* except (agency_name, agency_license_number),
            backup_matching.agency_name,
            backup_matching.agency_license_number,
        from null_agency_name
        left join
            agents_agencies as backup_matching
            on null_agency_name.agent_registration_number
            = backup_matching.agent_registration_number
        qualify
            row_number() over (
                partition by null_agency_name.property_transaction_key
                order by
                    case
                        -- match with the nearest value
                        when
                            null_agency_name.transaction_date
                            < backup_matching.agent_licence_validity_from
                        then
                            date_diff(
                                backup_matching.agent_licence_validity_from,
                                null_agency_name.transaction_date,
                                day
                            )
                        when
                            null_agency_name.transaction_date
                            > backup_matching.agent_licence_validity_to
                        then
                            date_diff(
                                null_agency_name.transaction_date,
                                backup_matching.agent_licence_validity_to,
                                day
                            )
                        else 9999  -- arbitrarily large number
                    end
            )
            = 1
    ),

    unioned as (
        select *
        from non_null_agency_name
        union all
        select *
        from null_agency_name_get_nearest_agency_match
    )

select *
from unioned
