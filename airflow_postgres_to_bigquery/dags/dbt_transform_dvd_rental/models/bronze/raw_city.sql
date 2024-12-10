SELECT city_id,
city,
country_id,
last_update
FROM {{source('public','city') }}