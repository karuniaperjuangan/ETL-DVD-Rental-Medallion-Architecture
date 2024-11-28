SELECT city_id, city 
FROM {{source('dvd_rental','city') }}