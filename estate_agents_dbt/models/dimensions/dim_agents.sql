with
    distinct_agents as (
        select
            agent_id,
            agent_registration_number,
            max_by(agent_name, last_updated_at_utc) as agent_name,
            array_agg(distinct coalesce(agent_alias, "")) as agent_aliases,
            min(agent_registration_validity_from) as agent_registration_validity_from,
            max(agent_registration_validity_to) as agent_registration_validity_to,
            agent_photo_url,
        from {{ ref("stg_estate_agents__agents") }}
        group by all
    ),

    mobile_numbers as (
        select distinct agent_id, agent_mobile_number
        from {{ ref("stg_estate_agents__mobile_numbers") }}
    ),

    joined as (
        select
            agents.agent_id,
            agents.agent_registration_number,
            agents.agent_name,
            agents.agent_aliases,
            agents.agent_registration_validity_from,
            agents.agent_registration_validity_to,
            agents.agent_photo_url,
            mobile_numbers.agent_mobile_number,
        from distinct_agents as agents
        left join mobile_numbers on mobile_numbers.agent_id = agents.agent_id
    )

select *
from joined
