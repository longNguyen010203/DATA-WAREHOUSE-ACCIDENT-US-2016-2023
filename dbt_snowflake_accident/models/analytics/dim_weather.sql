with source as (

    select
        -- uuid_string() as Weather_Id
        "Weather_Timestamp"
        , "Temperature(F)"
        , "Wind_Chill(F)"
        , "Humidity(%)"
        , "Pressure(in)"
        , "Visibility(mi)"
        , "Wind_Direction"
        , "Wind_Speed(mph)"
        , "Precipitation(in)"

    from {{ source('src_accident_vehicle_us', 'staging_accident') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key([
            '"Temperature(F)"',
            '"Wind_Chill(F)"',
            '"Humidity(%)"',
            '"Pressure(in)"',
            '"Visibility(mi)"'
        ]) }} as Weather_Key

        , "Weather_Timestamp"
        , "Temperature(F)"
        , "Wind_Chill(F)"
        , "Humidity(%)"
        , "Pressure(in)"
        , "Visibility(mi)"
        , "Wind_Direction"
        , "Wind_Speed(mph)"
        , "Precipitation(in)"

    from source

)

select * from renamed