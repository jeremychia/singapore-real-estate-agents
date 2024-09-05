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
            registrationnumber as agent_registration_number
        from source
    )
select *
from renamed
