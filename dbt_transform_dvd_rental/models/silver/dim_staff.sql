SELECT sta.staff_id, 
      first_name || ' ' || last_name AS full_name,
      ci.city AS city,
      co.country AS country,
      ad.postal_code AS postal_code,
      sta.active
FROM {{ ref('raw_staff') }} sta
LEFT JOIN {{ ref('raw_address') }} ad ON sta.address_id = ad.address_id
LEFT JOIN {{ ref('raw_city') }} ci ON ad.city_id = ci.city_id
LEFT JOIN {{ ref('raw_country') }} co ON ci.country_id = co.country_id