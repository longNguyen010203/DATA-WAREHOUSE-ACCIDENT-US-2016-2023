with source as (

    select
        -- uuid_string() as Severity_Id
        "Severity" as Severity_Level

    from {{ source('src_accident_vehicle_us', 'staging_accident') }}
    group by "Severity"

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['Severity_Level']) }} as Severity_Key
        , Severity_Level

    from source

)

select * from renamed