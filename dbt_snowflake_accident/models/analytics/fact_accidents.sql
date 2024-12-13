with accident_data as (

    select
        uuid_string() as Accident_ID
        , {{ dbt_utils.generate_surrogate_key(['accident."City"']) }} as City_Id
        , {{ dbt_utils.generate_surrogate_key(['accident."Street"']) }} as Street_Id
        , {{ dbt_utils.generate_surrogate_key(['accident."State"']) }} as State_Id
        , accident."Distance(mi)" as "Distance(mi)"
        , vehicle."Age_Band_of_Driver" as Age_Band_of_Driver
        , vehicle."Driver_Home_Area_Type" as Driver_Home_Area_Type
        , vehicle."Driver_IMD_Decile" as Driver_IMD_Decile
        , vehicle."Journey_Purpose_of_Driver" as Journey_Purpose_of_Driver
        , vehicle."Sex_of_Driver" as Sex_of_Driver
        , accident."Temperature(F)" as "Temperature(F)"
        , accident."Wind_Chill(F)" as "Wind_Chill(F)"
        , accident."Humidity(%)" as "Humidity(%)"
        , accident."Pressure(in)" as "Pressure(in)"
        , accident."Visibility(mi)" as "Visibility(mi)"
        , accident."Severity" as Severity_Level
        , accident."Start_Time" as Start_Time
        , accident."End_Time" as End_Time
        , accident."Timezone" as Timezone
        , accident."Weather_Condition" as Weather_Condition
        , vehicle."make" as Make
        , vehicle."model" as Model
        , vehicle."Age_of_Vehicle" as Age_of_Vehicle
        , vehicle."Engine_Capacity_CC" as Engine_Capacity_CC
        , vehicle."Propulsion_Code" as Propulsion_Code
        , vehicle."Vehicle_Type" as Vehicle_Type
        , vehicle."Was_Vehicle_Left_Hand_Drive" as Was_Vehicle_Left_Hand_Drive
        , vehicle."Skidding_and_Overturning" as Skidding_and_Overturning
        , vehicle."Towing_and_Articulation" as Towing_and_Articulation
        , vehicle."Vehicle_Leaving_Carriageway" as Vehicle_Leaving_Carriageway
        , vehicle."Vehicle_Location_Restricted_Lane" as Vehicle_Location_Restricted_Lane
        , vehicle."Vehicle_Manoeuvre" as Vehicle_Manoeuvre
        , vehicle."X1st_Point_of_Impact" as X1st_Point_of_Impact
        , accident."Sunrise_Sunset" as Sunrise_Sunset
        , accident."Civil_Twilight" as Civil_Twilight
        , accident."Nautical_Twilight" as Nautical_Twilight
        , accident."Astronomical_Twilight" as Astronomical_Twilight
        , accident."Amenity" as Amenity
        , accident."Bump" as Bump
        , accident."Crossing" as Crossing
        , accident."Give_Way" as Give_Way
        , accident."Junction" as Junction
        , accident."No_Exit" as No_Exit
        , accident."Railway" as Railway
        , accident."Roundabout" as Roundabout
        , accident."Station" as Station
        , accident."Stop" as Stop
        , accident."Traffic_Calming" as Traffic_Calming
        , accident."Traffic_Signal" as Traffic_Signal
        , accident."Turning_Loop" as Turning_Loop

    from {{ source('src_accident_vehicle_us', 'staging_accident') }} accident
        inner join {{ source('src_accident_vehicle_us', 'staging_vehicle') }} vehicle
            on accident."Accident_Index" = vehicle."Accident_Index"
    
),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['Accident_ID']) }} as Accident_Key
        , {{ dbt_utils.generate_surrogate_key(['Severity_Level']) }} as Severity_Id
        , {{ dbt_utils.generate_surrogate_key(['Street_Id', 'City_Id', 'State_Id']) }} as Location_Id
        , {{ dbt_utils.generate_surrogate_key(['Start_Time', 'End_Time', 'Timezone']) }} as Time_Id
        , {{ dbt_utils.generate_surrogate_key([
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
                'Turning_Loop']) }} as Poi_Id
        , {{ dbt_utils.generate_surrogate_key([
                'Age_Band_of_Driver',
                'Driver_Home_Area_Type',
                'Driver_IMD_Decile',
                'Journey_Purpose_of_Driver',
                'Sex_of_Driver']) }} as Driver_Id
        , {{ dbt_utils.generate_surrogate_key([
                '"Temperature(F)"',
                '"Wind_Chill(F)"',
                '"Humidity(%)"',
                '"Pressure(in)"',
                '"Visibility(mi)"']) }} as Weather_Id   
        , {{ dbt_utils.generate_surrogate_key([
                'Sunrise_Sunset', 
                'Civil_Twilight', 
                'Nautical_Twilight', 
                'Astronomical_Twilight']) }} as Astronomical_Periods_Id
        , {{ dbt_utils.generate_surrogate_key([
                'Make',
                'Model',
                'Age_of_Vehicle',
                'Engine_Capacity_CC',
                'Propulsion_Code',
                'Vehicle_Type',
                'Was_Vehicle_Left_Hand_Drive']) }} as Vehicle_Id
        , {{ dbt_utils.generate_surrogate_key([
                'Skidding_and_Overturning',
                'Towing_and_Articulation',
                'Vehicle_Leaving_Carriageway',
                'Vehicle_Location_Restricted_Lane',
                'Vehicle_Manoeuvre',
                'X1st_Point_of_Impact']) }} as Vehicle_Accident_Key
        , {{ dbt_utils.generate_surrogate_key(['Weather_Condition']) }} as Weather_Condition_Id
        , "Distance(mi)"

    from accident_data

)

select * from renamed