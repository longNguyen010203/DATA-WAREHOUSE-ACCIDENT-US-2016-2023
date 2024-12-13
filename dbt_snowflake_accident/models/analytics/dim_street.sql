with source as (

    select
        -- uuid_string() as Street_Id
        "Street" as Street_Name

    from {{ source('src_accident_vehicle_us', 'staging_accident') }}
    group by "Street"

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['Street_Name']) }} as Street_Key
        , Street_Name

    from source

)

select * from renamed