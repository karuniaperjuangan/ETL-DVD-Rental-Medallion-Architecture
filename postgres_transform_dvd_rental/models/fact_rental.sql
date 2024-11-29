SELECT r.rental_id, 
sta.staff_id, 
cu.customer_id,
sto.store_id,
f.film_id,
COALESCE(DATE_PART('day',r.return_date-r.rental_date),0) AS rental_days,
COALESCE(SUM(p.amount),0) AS total_payment_amount 
FROM {{source('dvd_rental','rental') }} r
LEFT JOIN {{source('dvd_rental','staff')}} sta ON r.staff_id = sta.staff_id
LEFT JOIN {{source('dvd_rental','store')}} sto ON sta.store_id = sto.store_id
LEFT JOIN {{source('dvd_rental','customer')}} cu ON r.customer_id = cu.customer_id
LEFT JOIN {{source('dvd_rental','inventory')}} i ON r.inventory_id = i.inventory_id
LEFT JOIN {{source('dvd_rental','film')}} f ON i.film_id = f.film_id
LEFT JOIN {{source('dvd_rental','payment')}} p ON r.rental_id = p.rental_id
GROUP BY 
r.rental_id, 
sta.staff_id, 
cu.customer_id,
sto.store_id,
f.film_id,
rental_days
ORDER BY rental_id