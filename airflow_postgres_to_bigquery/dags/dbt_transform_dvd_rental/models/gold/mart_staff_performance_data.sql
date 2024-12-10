SELECT sta.staff_id,
sta.full_name AS staff_full_name,
sta.city AS staff_city,
sta.country AS staff_country,
COALESCE(COUNT(r.rental_id),0) AS num_transactions,
COALESCE(SUM(r.total_payment_amount),0) AS total_revenue
FROM {{ ref('fact_rental') }} r
LEFT JOIN {{ ref('dim_staff') }} sta ON r.staff_id = sta.staff_id
GROUP BY sta.staff_id,
sta.full_name,
sta.city,
sta.country
ORDER BY sta.staff_id