with source as (

    select
        -- uuid_string() as City_Id
        accident."City" as City_Name
        , county.County_Key as County_Id

    from {{ source('src_accident_vehicle_us', 'staging_accident') }} accident
        inner join {{ ref("dim_county") }} county on accident."County" = county.County_Name
    group by
        accident."City",
        county.County_Key

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['City_Name']) }} as City_Key
        , County_Id
        , City_Name

    from source

)

select * from renamed