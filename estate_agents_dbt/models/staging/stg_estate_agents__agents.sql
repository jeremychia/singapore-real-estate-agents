with
    source as (select * from {{ source("estate_agents", "agents") }}),

    renamed as (
        select
            trim(id) as agent_id,
            trim(registrationnumber) as agent_registration_number,
            trim(name) as agent_name,
            trim(businessname) as agent_alias,
            date(validitydatestart) as agent_registration_validity_from,
            date(validitydateend) as agent_registration_validity_to,
            trim(awards) as agent_awards,  -- null field
            trim(disciplinaryactions) as agent_disciplinary_actions,  -- null field
            concat(
                'https://www.cea.gov.sg/aceas/api/internet/profile/v2/public-register/',
                trim(id),
                '/photo'
            ) as agent_photo_url,
            trim(currentea) as agency_name,
            trim(licensenumber) as agency_license_number,
            parse_timestamp(
                '%Y-%m-%d %H:%M:%S', _accessed_at_utc
            ) as last_updated_at_utc,
        from source
    )

select *
from renamed
