SELECT film_id,
category_id,
last_update 
FROM {{source('public','film_category') }}