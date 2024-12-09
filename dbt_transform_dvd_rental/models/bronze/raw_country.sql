SELECT country_id,
country,
last_update
FROM {{source('public','country') }}