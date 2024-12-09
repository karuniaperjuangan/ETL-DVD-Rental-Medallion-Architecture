SELECT f.film_id, title, description, release_year, 
rental_duration, rental_rate, length, replacement_cost, 
rating, special_features, fulltext,
lang.name AS language,
ca.name AS category
FROM {{ ref('raw_film') }} f
LEFT JOIN {{ ref('raw_film_category') }} fc ON f.film_id = fc.film_id
LEFT JOIN {{ ref('raw_category') }} ca ON fc.category_id = ca.category_id
LEFT JOIN {{ ref('raw_language') }} lang ON f.language_id = lang.language_id