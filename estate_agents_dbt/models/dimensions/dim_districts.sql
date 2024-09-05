with
    private_rental as (
        select distinct property_district_number, property_general_location
        from {{ ref("stg_estate_agents__private_rental") }}
    ),

    private_sale as (
        select distinct property_district_number, property_general_location
        from {{ ref("stg_estate_agents__private_sale") }}
    ),

    unioned_results as (
        select property_district_number, property_general_location
        from private_rental
        union distinct
        select property_district_number, property_general_location
        from private_sale
    )

select *
from unioned_results
order by property_district_number
