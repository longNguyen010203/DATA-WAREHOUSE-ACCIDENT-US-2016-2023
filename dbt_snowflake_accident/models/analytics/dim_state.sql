with source as (

    select
        -- uuid_string() as State_Id
        "State" as State_Name

    from {{ source('src_accident_vehicle_us', 'staging_accident') }}
    group by 
        "State"

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['State_Name']) }} as State_Key
        , State_Name

    from source

)

select * from renamed