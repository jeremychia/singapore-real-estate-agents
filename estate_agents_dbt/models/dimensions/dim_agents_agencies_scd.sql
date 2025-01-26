with
    stg_agents as (
        select
            agent_registration_number,
            agent_registration_validity_from,
            agent_registration_validity_to,
            agency_name,
            agency_license_number,
            last_updated_at_utc,
        from {{ ref("stg_estate_agents__agents") }}
    ),

    cleaning as (
        select
            agent_registration_number,
            agent_registration_validity_from as agent_licence_validity_from,
            coalesce(
                lead(agent_registration_validity_from) over (
                    partition by agent_registration_number order by last_updated_at_utc
                )
                - 1,
                agent_registration_validity_to
            ) as agent_licence_validity_to,
            agency_name,
            agency_license_number,
            last_updated_at_utc,
        from stg_agents
    )

select *
from cleaning
order by agent_registration_number, agent_licence_validity_from
