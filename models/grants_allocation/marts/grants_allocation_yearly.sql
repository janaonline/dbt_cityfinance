{{ config(
    materialized='table',
    tags=['grants_allocation', 'marts']
) }}

select
    year,
    city_type,
    cast(allocated_amount as numeric(18,2)) as allocated_amount,
    cast(released_amount as numeric(18,2)) as released_amount,
    cast("Un_Released_amount" as numeric(18,2)) as un_released_amount,
    cast(recommended_amount as numeric(18,2)) as recommended_amount,
    cast("percentage__of_released_Amount" as numeric(18,2)) as percentage_of_released_amount,
    cast("percentage__of_Un_released_Amount" as numeric(18,2)) as percentage_of_un_released_amount
from {{ source('cf_grants_allocation', 'grants_allocation_yearly') }}
