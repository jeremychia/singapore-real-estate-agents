with
    agents as (
        select
            agency_license_number,
            agency_name,
            agent_id,
            agent_registration_validity_from,
            agent_registration_validity_to
        from {{ ref("stg_estate_agents__agents") }}
    ),

    summarise_information as (
        select
            agency_license_number,
            agency_name,
            count(distinct agent_id) as count_agents,
            min(
                agent_registration_validity_from
            ) as earliest_agent_registration_validity_from,
            max(agent_registration_validity_to) as latest_agent_registration_validity_to
        from agents
        group by all
    )

select *
from summarise_information
