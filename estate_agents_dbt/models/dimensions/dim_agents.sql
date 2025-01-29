with
    data_gov_sg_backfill as (
        select 
            agent_registration_number,
            agent_name,
        from {{ ref("prep_agents_data_gov_sg") }}
    ),

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

    unioned as (
        select
            agent_id,
            agent_registration_number,
            agent_name,
            agent_aliases,
            agent_registration_validity_from,
            agent_registration_validity_to,
            agent_photo_url,
        from distinct_agents
        union all
        select
            null as agent_id,
            agent_registration_number,
            agent_name,
            null as agent_aliases,
            null as agent_registration_validity_from,
            null as agent_registration_validity_to,
            null as agent_photo_url,
        from data_gov_sg_backfill
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
        from unioned as agents
        left join mobile_numbers on mobile_numbers.agent_id = agents.agent_id
    )

select *
from joined
