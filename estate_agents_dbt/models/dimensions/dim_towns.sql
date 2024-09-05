with
    hdb_rental as (
        select distinct property_town from {{ ref("stg_estate_agents__hdb_rental") }}
    ),

    hdb_resale as (
        select distinct property_town from {{ ref("stg_estate_agents__hdb_resale") }}
    ),

    unioned_results as (
        select property_town
        from hdb_rental
        union distinct
        select property_town
        from hdb_resale
    )

select *
from unioned_results
order by property_town
