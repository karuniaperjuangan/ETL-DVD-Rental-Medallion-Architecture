SELECT category_id,
name,
last_update
FROM {{source('public','category') }}