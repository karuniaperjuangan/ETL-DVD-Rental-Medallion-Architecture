SELECT d.week, d.year,
COALESCE(COUNT(r.rental_id),0) AS num_transactions,
COALESCE(SUM(r.total_payment_amount),0) AS total_revenue
FROM {{ ref('fact_rental') }} r
LEFT JOIN {{ ref('dim_date') }} d ON r.rental_date_id = d.date_id
GROUP BY d.date_id, d.week, d.year
ORDER BY d.date_id