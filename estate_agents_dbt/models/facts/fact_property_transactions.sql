with
    union_relations as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("prep_property_transactions_scraped_website"),
                    ref("prep_property_transactions_data_gov_sg"),
                ],
                source_column_name=None,
            )
        }}
    ),

    deduplicated as (
        select *
        from union_relations
        qualify
            row_number() over (
                partition by property_transaction_key
                -- preference for those with transcation_id (from CEA scrapping)
                order by if(transaction_id is not null, 1, 0) desc
            )
            = 1
    ),

    reorder_columns as (
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
        from deduplicated
    )

select *
from reorder_columns
