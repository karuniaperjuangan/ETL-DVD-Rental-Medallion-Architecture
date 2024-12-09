SELECT cu.customer_id, 
cu.full_name AS customer_name,
cu.city AS customer_city,
cu.country AS customer_country,
COALESCE(COUNT(r.rental_id),0) AS num_transactions,
COALESCE(SUM(r.total_payment_amount),0) AS total_spending_amount
FROM {{ ref('fact_rental') }} r
LEFT JOIN {{ ref('dim_customer') }} cu ON r.customer_id = cu.customer_id
GROUP BY cu.customer_id, customer_name, customer_city, customer_country
ORDER BY customer_id