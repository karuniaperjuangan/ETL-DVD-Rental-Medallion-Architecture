SELECT film_id, title, description, release_year, rental_duration, rental_rate, length, replacement_cost, rating, special_features, fulltext
FROM {{source('dvd_rental','film') }}