with
    source as (select * from {{ source("estate_agents", "hdb_rental") }}),
    renamed as (
        select
            'hdb' as hdb_or_private,
            'rental' as rental_or_resale,
            id as transaction_id,
            date(transactiondate) as transaction_date,
            town as property_town,
            'HDB' as property_type,
            client,
            rentaltype as transaction_type,
            registrationnumber as agent_registration_number,
            _accessed_at_utc as last_updated_at_utc
        from source
    ),
    deduplicated as (
        select *
        from renamed
        qualify
            row_number() over (partition by transaction_id order by last_updated_at_utc)
            = 1
    )
select *
from deduplicated
