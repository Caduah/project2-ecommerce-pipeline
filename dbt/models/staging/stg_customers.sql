-- models/staging/stg_customers.sql

with source as (
    select * from {{ source('staging', 'customers') }}
),

renamed as (
    select
        customer_id,
        source_system,

        -- Normalised identity fields (used for joins & matching)
        full_name_norm                          as customer_name,
        email_normalised                        as email,
        phone_normalised                        as phone,
        last_name_soundex,

        -- Address
        city,
        state_province,
        postal_code,
        country,

        -- Demographics
        age,
        age_band,

        -- Segmentation
        segment,
        loyalty_tier,
        is_active,

        -- Tenure
        registration_ts,
        days_since_registration,
        customer_tenure_band,

        -- Entity resolution (filled in Phase 6)
        coalesce(resolved_entity_id, customer_id) as resolved_entity_id,
        er_confidence,

        -- Audit
        silver_ts,
        pipeline_version

    from source
    where customer_id is not null
)

select * from renamed
