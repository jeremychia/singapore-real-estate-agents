with
    source as (
        select * from {{ ref("stg_data_gov_sg__property_transactions_backfill") }}
    ),

    distinct_agents as (
        select distinct agent_registration_number, agent_name
        from source
        where agent_registration_number != '-' and agent_name != '-'
    ),

    find_agents_not_in_scraped as (
        select *
        from distinct_agents
        where
            agent_registration_number not in (
                select distinct agent_registration_number
                from {{ ref("stg_estate_agents__agents") }}
            )
    )

select *
from find_agents_not_in_scraped
