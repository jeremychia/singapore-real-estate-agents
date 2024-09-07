with
    hdb_rental as (
        select distinct property_town from {{ ref("stg_estate_agents__hdb_rental") }}
    ),

    hdb_resale as (
        select distinct property_town from {{ ref("stg_estate_agents__hdb_resale") }}
    ),

    town_mapping as (
        select property_town, region, central_region_category from {{ ref("towns") }}
    ),

    unioned_results as (
        select property_town
        from hdb_rental
        union distinct
        select property_town
        from hdb_resale
    ),

    join_mapping as (
        select unioned_results.*, mapping.region, mapping.central_region_category
        from unioned_results
        left join
            town_mapping as mapping
            on unioned_results.property_town = mapping.property_town
    )

select *
from join_mapping
where property_town is not null
order by property_town
