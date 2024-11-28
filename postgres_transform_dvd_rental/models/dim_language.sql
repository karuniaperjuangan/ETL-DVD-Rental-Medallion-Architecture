SELECT language_id, name 
FROM {{source('dvd_rental','language') }}