with
    private_rental as (
        select distinct property_district_number, property_general_location
        from {{ ref("stg_estate_agents__private_rental") }}
    ),

    private_sale as (
        select distinct property_district_number, property_general_location
        from {{ ref("stg_estate_agents__private_sale") }}
    ),

    district_mapping as (
        select property_district_number, region, central_region_category
        from {{ ref("districts") }}
    ),

    unioned_results as (
        select property_district_number, property_general_location
        from private_rental
        union distinct
        select property_district_number, property_general_location
        from private_sale
    ),

    join_mapping as (
        select unioned_results.*, mapping.region, mapping.central_region_category
        from unioned_results
        left join
            district_mapping as mapping
            on unioned_results.property_district_number
            = mapping.property_district_number
    )

select *
from join_mapping
where property_district_number is not null
order by property_district_number
