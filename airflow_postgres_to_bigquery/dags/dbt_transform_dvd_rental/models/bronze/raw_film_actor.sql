SELECT actor_id,
film_id,
last_update
FROM {{source('public','film_actor') }}