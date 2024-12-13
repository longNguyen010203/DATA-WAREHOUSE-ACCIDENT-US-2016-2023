with source as (

    select
        -- uuid_string() as Astronomical_Periods_Id
        "Sunrise_Sunset" as Sunrise_Sunset
        , "Civil_Twilight" as Civil_Twilight
        , "Nautical_Twilight" as Nautical_Twilight
        , "Astronomical_Twilight" as Astronomical_Twilight

    from {{ source('src_accident_vehicle_us', 'staging_accident') }}
    -- group by 
    --     "Sunrise_Sunset",
    --     "Civil_Twilight",
    --     "Nautical_Twilight",
    --     "Astronomical_Twilight"

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'Sunrise_Sunset', 
            'Civil_Twilight', 
            'Nautical_Twilight', 
            'Astronomical_Twilight'
        ]) }} as Astronomical_Periods_Key
        
        , Sunrise_Sunset
        , Civil_Twilight
        , Nautical_Twilight
        , Astronomical_Twilight

    from source

)

select * from renamed