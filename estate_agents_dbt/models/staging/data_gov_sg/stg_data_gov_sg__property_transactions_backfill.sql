{% set property_type_map = {
    "CONDOMINIUM_APARTMENTS": "Condominium/Apartments",
    "EXECUTIVE_CONDOMINIUM": "Executive Condominium",
    "HDB": "HDB",
    "LANDED": "Landed",
    "STRATA_LANDED": "Strata-Landed"
} %}

{% set transaction_type_map = {
    "NEW SALE": "New Sale",
    "RESALE": "Resale",
    "SUB-SALE": "Sub-sale",
    "ROOM RENTAL": "Room Rental",
    "WHOLE RENTAL": "Whole Rental"
} %}

{% set represented_map = {
    "BUYER": "Buyer",
    "SELLER": "Seller",
    "TENANT": "Tenant",
    "LANDLORD": "Landlord"
} %}

with source as (
    select *
    from {{ source("data_gov_sg", "property_transactions_backfill") }}
),

cleaned as (
    select
        trim(salesperson_name) as salesperson_name,
        parse_date('%b-%Y', transaction_date) as transaction_date,
        trim(salesperson_reg_num) as salesperson_reg_num,
        
        case 
            {% for key, value in property_type_map.items() %}
                when property_type = '{{ key }}' then '{{ value }}'
            {% endfor %}
        end as property_type,
        
        case 
            {% for key, value in transaction_type_map.items() %}
                when transaction_type = '{{ key }}' then '{{ value }}'
            {% endfor %}
        end as transaction_type,
        
        case 
            {% for key, value in represented_map.items() %}
                when represented = '{{ key }}' then '{{ value }}'
            {% endfor %}
        end as represented,
        
        if(town = '-', null, trim(town)) as town,
        if(district = '-', null, cast(district as int)) as district,
        if(general_location = '-', null, trim(general_location)) as general_location
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
