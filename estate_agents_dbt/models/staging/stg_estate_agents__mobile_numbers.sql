with
    source as (select * from {{ source("estate_agents", "mobile_numbers") }}),
    renamed as (
        select
            mobile_number as agent_mobile_number,
            trim(id) as agent_id,
            trim(registration_number) as agent_registration_number,
            _accessed_at_utc as mobile_number_last_updated_at
        from source
    ),
    deduplicated as (
        select *
        from renamed
        qualify
            row_number() over (
                partition by agent_registration_number
                order by mobile_number_last_updated_at desc
            )
            = 1
    )
select *
from deduplicated
