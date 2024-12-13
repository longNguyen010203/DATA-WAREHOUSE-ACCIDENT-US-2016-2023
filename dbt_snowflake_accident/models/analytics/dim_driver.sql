with source as (

    select
        -- uuid_string() as Driver_Id
        "Age_Band_of_Driver" as Age_Band_of_Driver
        , "Driver_Home_Area_Type" as Driver_Home_Area_Type
        , "Driver_IMD_Decile" as Driver_IMD_Decile
        , "Journey_Purpose_of_Driver" as Journey_Purpose_of_Driver
        , "Sex_of_Driver" as Sex_of_Driver

    from {{ source('src_accident_vehicle_us', 'staging_vehicle') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'Age_Band_of_Driver',
            'Driver_Home_Area_Type',
            'Driver_IMD_Decile',
            'Journey_Purpose_of_Driver',
            'Sex_of_Driver'
        ]) }} as Driver_Key
        
        , Age_Band_of_Driver
        , Driver_Home_Area_Type
        , Driver_IMD_Decile
        , Journey_Purpose_of_Driver
        , Sex_of_Driver

    from source

)

select * from renamed