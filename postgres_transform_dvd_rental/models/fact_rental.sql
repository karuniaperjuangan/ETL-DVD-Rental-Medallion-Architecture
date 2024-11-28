SELECT r.rental_id, 
s.staff_id, 
cu.customer_id,
cat.category_id,
f.film_id,
lang.language_id,
ci.city_id,
co.country_id,
COALESCE(DATE_PART('day',r.return_date-r.rental_date),0) AS rental_days,
COALESCE(SUM(p.amount),0) AS total_payment_amount 
FROM {{source('dvd_rental','rental') }} r
LEFT JOIN {{source('dvd_rental','staff')}} s USING (staff_id)
LEFT JOIN {{source('dvd_rental','customer')}} cu USING (customer_id)
LEFT JOIN {{source('dvd_rental','inventory')}} i USING (inventory_id)
LEFT JOIN {{source('dvd_rental','film')}} f ON i.film_id = f.film_id
LEFT JOIN {{source('dvd_rental','film_category')}} fc ON f.film_id = fc.film_id
LEFT JOIN {{source('dvd_rental','category')}} cat ON fc.category_id = cat.category_id
LEFT JOIN {{source('dvd_rental','language')}} lang ON f.language_id = lang.language_id
LEFT JOIN {{source('dvd_rental','address')}} ad ON cu.address_id = ad.address_id
LEFT JOIN {{source('dvd_rental','city')}} ci ON ad.city_id = ci.city_id
LEFT JOIN {{source('dvd_rental','country')}} co ON ci.country_id = co.country_id
LEFT JOIN {{source('dvd_rental','payment')}} p ON r.rental_id = p.rental_id
GROUP BY 
r.rental_id, 
s.staff_id, 
cu.customer_id,
cat.category_id,
f.film_id,
lang.language_id,
ci.city_id,
co.country_id,
rental_days
ORDER BY rental_id