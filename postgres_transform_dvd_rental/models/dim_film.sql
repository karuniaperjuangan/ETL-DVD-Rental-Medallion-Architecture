SELECT f.film_id, title, description, release_year, 
rental_duration, rental_rate, length, replacement_cost, 
rating, special_features, fulltext,
lang.name AS language,
ca.name AS category
FROM {{source('dvd_rental','film') }} f
LEFT JOIN {{source('dvd_rental','film_category') }} fc ON f.film_id = fc.film_id
LEFT JOIN {{source('dvd_rental','category') }} ca ON fc.category_id = ca.category_id
LEFT JOIN {{source('dvd_rental','language') }} lang ON f.language_id = lang.language_id