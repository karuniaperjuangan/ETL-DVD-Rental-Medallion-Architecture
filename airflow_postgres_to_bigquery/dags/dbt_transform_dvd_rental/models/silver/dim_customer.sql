SELECT cu.customer_id, 
      first_name || ' ' || last_name AS full_name,
      ci.city AS city,
      co.country AS country,
      ad.postal_code AS postal_code,
      cu.active
FROM {{ ref('raw_customer') }} cu
LEFT JOIN {{ ref('raw_address') }} ad ON cu.address_id = ad.address_id
LEFT JOIN {{ ref('raw_city') }} ci ON ad.city_id = ci.city_id
LEFT JOIN {{ ref('raw_country') }} co ON ci.country_id = co.country_id