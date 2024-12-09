SELECT r.rental_id, 
sta.staff_id, 
cu.customer_id,
sto.store_id,
f.film_id,
ren_date.date_id AS rental_date_id,
ret_date.date_id AS return_date_id,
COALESCE(DATE_PART('day',r.return_date-r.rental_date),0) AS rental_days,
COALESCE(SUM(p.amount),0) AS total_payment_amount 
FROM {{ ref('raw_rental') }} r
LEFT JOIN {{ ref('raw_staff')}} sta ON r.staff_id = sta.staff_id
LEFT JOIN {{ ref('raw_store')}} sto ON sta.store_id = sto.store_id
LEFT JOIN {{ ref('raw_customer')}} cu ON r.customer_id = cu.customer_id
LEFT JOIN {{ ref('raw_inventory')}} i ON r.inventory_id = i.inventory_id
LEFT JOIN {{ ref('raw_film')}} f ON i.film_id = f.film_id
LEFT JOIN {{ ref('raw_payment')}} p ON r.rental_id = p.rental_id
LEFT JOIN {{ ref('dim_date')}} ren_date ON DATE_TRUNC('day',r.rental_date) = ren_date.timestamp
LEFT JOIN {{ ref('dim_date')}} ret_date ON DATE_TRUNC('day',r.return_date)= ret_date.timestamp
GROUP BY 
r.rental_id, 
sta.staff_id, 
cu.customer_id,
sto.store_id,
f.film_id,
rental_date_id,
return_date_id,
rental_days
ORDER BY rental_id