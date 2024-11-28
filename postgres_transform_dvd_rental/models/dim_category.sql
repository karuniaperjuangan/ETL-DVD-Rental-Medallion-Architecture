SELECT category_id, name 
FROM {{source('dvd_rental','category') }}