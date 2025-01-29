with
    source as (
        select * from {{ ref("stg_data_gov_sg__property_transactions_backfill") }}
    ),

    -- complete data from scrapped source starts from oct 2022
    before_oct_2022 as (select * from source where transaction_month < "2022-10-01"),

    on_or_after_oct_2022 as (
        select *
        from source
        where
            transaction_month >= "2022-10-01"
            -- suspect that there are duplicates in the dataset, so will only rely on
            -- the scraped data for periods afterwards
            and agent_registration_number not in (
                select distinct agent_registration_number
                from {{ ref("prep_property_transactions_scraped_website") }}
                where transaction_month >= "2022-10-01"
            )
    ),

    unioned as (
        select *
        from before_oct_2022
        union all
        select *
        from on_or_after_oct_2022
    )

select *
from unioned
