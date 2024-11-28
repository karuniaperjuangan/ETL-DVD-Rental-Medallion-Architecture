SELECT staff_id, 
      first_name || ' ' || last_name AS full_name,
      email,
      username,
      password,
      picture,
      active
FROM {{source('dvd_rental','staff') }}