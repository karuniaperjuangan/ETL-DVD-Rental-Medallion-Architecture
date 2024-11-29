SELECT sto.store_id, 
      mn.first_name || ' '|| mn.last_name AS manager_full_name,
      ci.city AS city,
      co.country AS country,
      ad.postal_code AS postal_code
FROM {{source('dvd_rental','store') }} sto
LEFT JOIN {{source('dvd_rental','staff') }} mn ON sto.manager_staff_id = mn.staff_id
LEFT JOIN {{source('dvd_rental','address') }} ad ON sto.address_id = ad.address_id
LEFT JOIN {{source('dvd_rental','city') }} ci ON ad.city_id = ci.city_id
LEFT JOIN {{source('dvd_rental','country') }} co ON ci.country_id = co.country_id
