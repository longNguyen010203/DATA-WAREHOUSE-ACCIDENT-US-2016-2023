with source as (

    select
        -- uuid_string() as Weather_Condition_Id
        "Weather_Condition" as Weather_Condition

    from {{ source('src_accident_vehicle_us', 'staging_accident') }}
    group by "Weather_Condition"

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['Weather_Condition']) }} as Weather_Condition_Key
        , Weather_Condition

    from source

)

select * from renamed