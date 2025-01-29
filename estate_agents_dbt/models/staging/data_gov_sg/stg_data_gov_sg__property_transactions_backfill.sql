with
    source as (
        select *
        from {{ source("data_gov_sg", "property_transactions_backfill") }}
    ),

    cleaned as (
        select
            trim(salesperson_name) as salesperson_name,
            parse_date('%b-%Y', transaction_date) as transaction_date,
            trim(salesperson_reg_num) as salesperson_reg_num,
            case
                when property_type = "CONDOMINIUM_APARTMENTS" then "Condominium/Apartments"
                when property_type = "EXECUTIVE_CONDOMINIUM" then "Executive Condominium"
                when property_type = "HDB" then "HDB"
                when property_type = "LANDED" then "Landed"
                when property_type = "STRATA_LANDED" then "Strata-Landed"
            end as property_type,
            case
                when transaction_type="NEW SALE" then "New Sale"
                when transaction_type="RESALE" then "Resale"
                when transaction_type="SUB-SALE" then "Sub-sale"
                when transaction_type="ROOM RENTAL" then "Room Rental"
                when transaction_type="WHOLE RENTAL" then "Whole Rental"
            end as transaction_type,
            case
                when represented="BUYER" then "Buyer"
                when represented="SELLER" then "Seller"
                when represented="TENANT" then "Tenant"
                when represented="LANDLORD" then "Landlord"
            end as represented,
            if(town = "-", null, trim(town)) as town,
            if(district="-", null, cast(district as int)) as district,
            if(general_location = "-", null, trim(general_location)) as general_location,
        from source
    ),

    renamed as (
        select
            salesperson_name as agent_name,
            transaction_date as transaction_month,
            salesperson_reg_num as agent_registration_number,
            property_type,
            transaction_type,
            represented as client,
            town as property_town,
            district as property_district_number,
            general_location as property_general_location
        from cleaned
    )

select *
from renamed
