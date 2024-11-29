SELECT sta.staff_id, 
      first_name || ' ' || last_name AS full_name,
      ci.city AS city,
      co.country AS country,
      ad.postal_code AS postal_code,
      sta.active
FROM {{source('dvd_rental','staff') }} sta
LEFT JOIN {{source('dvd_rental','address') }} ad ON sta.address_id = ad.address_id
LEFT JOIN {{source('dvd_rental','city') }} ci ON ad.city_id = ci.city_id
LEFT JOIN {{source('dvd_rental','country') }} co ON ci.country_id = co.country_id