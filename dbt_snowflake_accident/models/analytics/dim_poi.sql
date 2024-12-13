with source as (

    select
        -- uuid_string() as Poi_Id
        "Amenity" as Amenity
        , "Bump" as Bump
        , "Crossing" as Crossing
        , "Give_Way" as Give_Way
        , "Junction" as Junction
        , "No_Exit" as No_Exit
        , "Railway" as Railway
        , "Roundabout" as Roundabout
        , "Station" as Station
        , "Stop" as Stop
        , "Traffic_Calming" as Traffic_Calming
        , "Traffic_Signal" as Traffic_Signal
        , "Turning_Loop" as Turning_Loop

    from {{ source('src_accident_vehicle_us', 'staging_accident') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'Amenity',
            'Bump',
            'Crossing',
            'Give_Way',
            'Junction',
            'No_Exit',
            'Railway',
            'Roundabout',
            'Station',
            'Stop',
            'Traffic_Calming',
            'Traffic_Signal',
            'Turning_Loop'
        ]) }} as Poi_Key

        , Amenity
        , Bump
        , Crossing
        , Give_Way
        , Junction
        , No_Exit
        , Railway
        , Roundabout
        , Station
        , Stop
        , Traffic_Calming
        , Traffic_Signal
        , Turning_Loop

    from source

)

select * from renamed