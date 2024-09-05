with
    agents as (
        select
            agent_id,
            agent_registration_number,
            agent_name,
            agent_alias,
            agent_registration_validity_from,
            agent_registration_validity_to,
            agent_photo_url,
            agency_license_number,
            agency_name
        from {{ ref("stg_estate_agents__agents") }}
    ),

    mobile_numbers as (
        select agent_id, agent_mobile_number
        from {{ ref("stg_estate_agents__mobile_numbers") }}
    ),

    joined as (
        select
            agents.agent_id,
            agents.agent_registration_number,
            agents.agent_name,
            agents.agent_alias,
            agents.agent_registration_validity_from,
            agents.agent_registration_validity_to,
            agents.agent_photo_url,
            mobile_numbers.agent_mobile_number,
            agents.agency_license_number,
            agents.agency_name
        from agents
        left join mobile_numbers on mobile_numbers.agent_id = agents.agent_id
    )

select *
from joined
