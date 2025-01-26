with
    get_changes as (
        select
            agent_registration_number,
            lead(agent_licence_validity_from) over (
                partition by agent_registration_number
                order by agent_licence_validity_from
            ) as licence_change_date,
            agency_name,
            lead(agency_name) over (
                partition by agent_registration_number
                order by agent_licence_validity_from
            ) as next_agency_name
        from {{ ref("dim_agents_agencies_scd") }}
        qualify
            lead(agency_name) over (
                partition by agent_registration_number
                order by agent_licence_validity_from
            )
            is not null
    ),
    joined as (
        select
            get_changes.agent_registration_number,
            dim_agents.agent_name,
            dim_agents.agent_photo_url,
            get_changes.licence_change_date,
            get_changes.agency_name,
            get_changes.next_agency_name,
        from get_changes
        left join
            {{ ref("dim_agents") }} as dim_agents using (agent_registration_number)
    )

select *
from joined
order by licence_change_date desc
