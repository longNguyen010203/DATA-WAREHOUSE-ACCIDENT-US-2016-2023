with source as (

    select
        -- uuid_string() as Vehicle_Accident_Id
        "Skidding_and_Overturning" as Skidding_and_Overturning
        , "Towing_and_Articulation" as Towing_and_Articulation
        , "Vehicle_Leaving_Carriageway" as Vehicle_Leaving_Carriageway
        , "Vehicle_Location_Restricted_Lane" as Vehicle_Location_Restricted_Lane
        , "Vehicle_Manoeuvre" as Vehicle_Manoeuvre
        , "X1st_Point_of_Impact" as X1st_Point_of_Impact

    from {{ source('src_accident_vehicle_us', 'staging_vehicle') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'Skidding_and_Overturning',
            'Towing_and_Articulation',
            'Vehicle_Leaving_Carriageway',
            'Vehicle_Location_Restricted_Lane',
            'Vehicle_Manoeuvre',
            'X1st_Point_of_Impact'
        ]) }} as Vehicle_Accident_Key

        , Skidding_and_Overturning
        , Towing_and_Articulation
        , Vehicle_Leaving_Carriageway
        , Vehicle_Location_Restricted_Lane
        , Vehicle_Manoeuvre
        , X1st_Point_of_Impact

    from source

)

select * from renamed