SELECT customer_id, 
      first_name || ' ' || last_name AS full_name,
      email,
      active
FROM {{source('dvd_rental','customer') }}