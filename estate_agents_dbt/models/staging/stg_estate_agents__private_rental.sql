with
    source as (select * from {{ source("estate_agents", "private_rental") }}),
    renamed as (
        select
            'private' as hdb_or_private,
            'rental' as rental_or_resale,
            id as transaction_id,
            date(transactiondate) as transaction_date,
            district as property_district_number,
            generallocation as property_general_location,
            property as property_type,
            client,
            rentaltype as transaction_type,
            registrationnumber as agent_registration_number
        from source
    )

select *
from renamed
