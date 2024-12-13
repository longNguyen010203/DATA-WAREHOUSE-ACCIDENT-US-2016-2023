with source as (

    select
        -- uuid_string() as Time_Id
        "Start_Time" as Start_Time
        , "End_Time" as End_Time
        , "Timezone" as Timezone

    from {{ source('src_accident_vehicle_us', 'staging_accident') }}
    group by 
        Start_Time,
        End_Time,
        Timezone
        
),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['Start_Time', 'End_Time', 'Timezone']) }} as Time_Key
        , Start_Time
        , End_Time
        , Timezone

    from source

)

select * from renamed