with source as (

    select
        -- uuid_string() as Vehicle_Id
        "make" as Make
        , "model" as Model
        , "Age_of_Vehicle" as Age_of_Vehicle
        , "Engine_Capacity_CC" as Engine_Capacity_CC
        , "Propulsion_Code" as Propulsion_Code
        , "Vehicle_Type" as Vehicle_Type
        , "Was_Vehicle_Left_Hand_Drive" as Was_Vehicle_Left_Hand_Drive

    from {{ source('src_accident_vehicle_us', 'staging_vehicle') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'Make',
            'Model',
            'Age_of_Vehicle',
            'Engine_Capacity_CC',
            'Propulsion_Code',
            'Vehicle_Type',
            'Was_Vehicle_Left_Hand_Drive'
        ]) }} as Vehicle_Key

        , Make
        , Model
        , Age_of_Vehicle
        , Engine_Capacity_CC
        , Propulsion_Code
        , Vehicle_Type
        , Was_Vehicle_Left_Hand_Drive

    from source

)

select * from renamed