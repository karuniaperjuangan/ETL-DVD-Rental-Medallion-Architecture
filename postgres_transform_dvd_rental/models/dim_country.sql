SELECT country_id, country 
FROM {{source('dvd_rental','country') }}