with
    source as (select * from {{ source("estate_agents", "hdb_resale") }}),
    renamed as (
        select
            'hdb' as hdb_or_private,
            'resale' as rental_or_resale,
            id as transaction_id,
            date(transactiondate) as transaction_date,
            town as property_town,
            'HDB' as property_type,
            client,
            'Resale' as transaction_type,
            registrationnumber as agent_registration_number
        from source
    )
select *
from renamed
