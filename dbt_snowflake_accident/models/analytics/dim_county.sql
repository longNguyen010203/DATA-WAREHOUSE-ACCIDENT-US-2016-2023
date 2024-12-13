with source as (

    select
        -- uuid_string() as County_Id
        "County" as County_Name

    from {{ source('src_accident_vehicle_us', 'staging_accident') }}
    group by "County"

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['County_Name']) }} as County_Key
        , County_Name

    from source

)

select * from renamed