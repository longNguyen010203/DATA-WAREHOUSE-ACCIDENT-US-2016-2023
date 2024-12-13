with source as (

    select
        -- uuid_string() as Location_Id
        {{ dbt_utils.generate_surrogate_key(['"Street"']) }} as Street_Id
        , {{ dbt_utils.generate_surrogate_key(['"City"']) }} as City_Id
        , {{ dbt_utils.generate_surrogate_key(['"State"']) }} as State_Id
        -- , street.Street_Key as Street_Id
        -- , city.City_Key as City_Id
        -- , state.State_Key as State_Id
        , accident."Start_Lat" as Start_Lat
        , accident."Start_Lng" as Start_Lng
        , accident."End_Lat" as End_Lat
        , accident."End_Lng" as End_Lng
        , accident."Zipcode" as Zipcode
        , accident."Country" as Country
        , vehicle."Junction_Location" as Junction_Location

    from {{ source('src_accident_vehicle_us', 'staging_accident') }} accident
        -- inner join {{ ref("dim_state") }} state on accident."State" = state.State_Name
        -- inner join {{ ref("dim_street") }} street on accident."Street" = street.Street_Name
        -- inner join {{ ref("dim_city") }} city on accident."City" = city.City_Name
        inner join {{ source('src_accident_vehicle_us', 'staging_vehicle') }} vehicle
            on accident."Accident_Index" = vehicle."Accident_Index"
    -- group by
    --     Street_Id, 
    --     City_Id, 
    --     State_Id, 
    --     Start_Lat, 
    --     Start_Lng, 
    --     End_Lat, 
    --     End_Lng, 
    --     Zipcode, 
    --     Country, 
    --     Junction_Location
    -- limit 100

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['Street_Id', 'City_Id', 'State_Id']) }} as Location_Key
        , Street_Id
        , City_Id
        , State_Id
        , Start_Lat
        , Start_Lng
        , End_Lat
        , End_Lng
        , Zipcode
        , Country
        , Junction_Location

    from source

)

select * from renamed