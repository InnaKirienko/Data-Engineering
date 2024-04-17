/*
 Завдання на SQL до лекції 03.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/

SELECT  c.name              AS category,
        count(fc. film_id)  AS films_count
FROM film_category fc
        JOIN category c ON fc.category_id = c.category_id
GROUP BY category
ORDER BY films_count DESC
;

/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/

SELECT  a.first_name|| ' ' || a.last_name   AS actor_name,
        count(r.rental_id)                  AS rental_count
FROM film_actor fa
        JOIN actor a ON a.actor_id = fa.actor_id
        JOIN film f ON f.film_id = fa.film_id
        JOIN inventory i ON f.film_id = i.film_id
        JOIN rental r ON i.inventory_id = r.inventory_id
GROUP BY actor_name
ORDER BY (rental_count) DESC
LIMIT 10
;


/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/

SELECT  c.name          AS category,
        sum(p. amount)  AS total_amount
FROM payment p
        JOIN rental r ON p.rental_id = r.rental_id
        JOIN inventory i ON r.inventory_id = i.inventory_id
        JOIN film f ON i.film_id = f.film_id
        JOIN film_category fc ON f.film_id = fc.film_id
        JOIN category c ON fc.category_id = c.category_id
GROUP BY category
ORDER BY total_amount DESC
LIMIT 1
;


/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/

SELECT DISTINCT f.title AS films_are_not_in_inventory
FROM film f
        LEFT OUTER JOIN inventory i ON f.film_id = i.film_id
WHERE i.film_id IS NULL
;

/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/

SELECT  a.first_name|| ' ' || a.last_name   AS actor_name,
        count(c.name)                       AS count_in_category
FROM film_actor fa
        JOIN actor a ON a.actor_id = fa.actor_id
        JOIN film f ON f.film_id = fa.film_id
        JOIN film_category fc ON f.film_id = fc.film_id
        JOIN category c ON fc.category_id = c.category_id
WHERE c.name = 'Children'
GROUP BY actor_name
ORDER BY count_in_category DESC
LIMIT 3
;