SELECT sto.store_id, 
      mn.first_name || ' '|| mn.last_name AS manager_full_name,
      ci.city AS city,
      co.country AS country,
      ad.postal_code AS postal_code
FROM {{ ref('raw_store') }} sto
LEFT JOIN {{ ref('raw_staff') }} mn ON sto.manager_staff_id = mn.staff_id
LEFT JOIN {{ ref('raw_address') }} ad ON sto.address_id = ad.address_id
LEFT JOIN {{ ref('raw_city') }} ci ON ad.city_id = ci.city_id
LEFT JOIN {{ ref('raw_country') }} co ON ci.country_id = co.country_id
