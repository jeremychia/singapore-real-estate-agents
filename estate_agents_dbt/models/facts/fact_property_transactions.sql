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
    )

select *
from reorder_columns
