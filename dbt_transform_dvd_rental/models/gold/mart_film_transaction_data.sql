SELECT f.film_id,
f.title,
f.language,
f.category AS genre,
COALESCE(COUNT(r.rental_id),0) AS num_transactions,
COALESCE(SUM(r.total_payment_amount),0) AS total_revenue
FROM {{ ref('fact_rental') }} r
LEFT JOIN {{ ref('dim_film') }} f ON r.film_id = f.film_id
GROUP BY f.film_id,
f.title,
f.language,
f.category
ORDER BY f.film_id