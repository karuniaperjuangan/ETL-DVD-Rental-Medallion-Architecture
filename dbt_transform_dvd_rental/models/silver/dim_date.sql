WITH rental_date_table AS (
    SELECT 
    DATE_TRUNC('day',r.rental_date) AS timestamp,
    DATE_PART('year',r.rental_date) AS year,
    DATE_PART('quarter',r.rental_date) AS quarter,
    DATE_PART('month',r.rental_date) AS month,
    DATE_PART('week',r.rental_date) AS week,
    DATE_PART('isodow',r.rental_date) AS day_of_week,
    DATE_PART('day',r.rental_date) AS day
    FROM {{ ref('raw_rental') }} r
), return_date_table AS (
    SELECT 
    DATE_TRUNC('day',r.return_date) AS timestamp,
    DATE_PART('year',r.return_date) AS year,
    DATE_PART('quarter',r.return_date) AS quarter,
    DATE_PART('month',r.return_date) AS month,
    DATE_PART('week',r.return_date) AS week,
    DATE_PART('isodow',r.return_date) AS day_of_week,
    DATE_PART('day',r.return_date) AS day
    FROM {{ ref('raw_rental') }} r
)
SELECT 
ROW_NUMBER() OVER (ORDER BY year, month, day) AS date_id,
timestamp,
year,
quarter,
month,
week,
day_of_week,
day
FROM (SELECT * FROM rental_date_table UNION SELECT * FROM return_date_table)