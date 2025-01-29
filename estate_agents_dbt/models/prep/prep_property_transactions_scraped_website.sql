with
    union_relations as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("stg_estate_agents__hdb_rental"),
                    ref("stg_estate_agents__hdb_resale"),
                    ref("stg_estate_agents__private_rental"),
                    ref("stg_estate_agents__private_sale"),
                ],
                source_column_name=None,
            )
        }}
    ),

    reorder_columns as (
        select
            transaction_id,
            transaction_date,
            date_trunc(transaction_date, month) as transaction_month,
            transaction_type,
            hdb_or_private,
            rental_or_resale,
            property_town,
            property_district_number,
            property_general_location,
            property_type,
            client,
            agent_registration_number
        from union_relations
    ),

    add_property_transaction_key as (
        select
            *,
            md5(
                concat(
                    coalesce(cast(transaction_month as string), ""),
                    coalesce(trim(agent_registration_number), ""),
                    coalesce(trim(property_type), ""),
                    coalesce(trim(transaction_type), ""),
                    coalesce(trim(client), ""),
                    coalesce(trim(property_town), ""),
                    coalesce(cast(property_district_number as string), ""),
                    coalesce(trim(property_general_location), ""),
                    -- consider that there may be more than one listing with these
                    -- attributes in the same month
                    cast(
                        row_number() over (
                            partition by
                                transaction_month,
                                agent_registration_number,
                                property_type,
                                transaction_type,
                                client,
                                property_town,
                                property_district_number,
                                property_general_location
                        ) as string
                    )
                )
            ) as property_transaction_key,
        from reorder_columns
    )

select *
from add_property_transaction_key
