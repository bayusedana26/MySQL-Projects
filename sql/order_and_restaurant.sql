SELECT * FROM orders_data od 

SELECT * FROM restaurants_data rd 

SELECT
	restaurant_id,
	restaurant_name,
	MAX(total_order)
FROM
	restaurants_data
JOIN orders_data ON
	restaurants_data.id = orders_data.id
GROUP BY 
	restaurant_id, restaurant_name
 
SELECT
    rd.id AS restaurant_id,
    rd.restaurant_name,
    MAX(od.avg_total_order) AS max_avg_total_order
FROM
    restaurants_data rd
JOIN (
    SELECT
        restaurant_id,
        AVG(total_order) AS avg_total_order
    FROM
        orders_data
    GROUP BY
        restaurant_id
) od ON rd.id = od.restaurant_id
GROUP BY
    rd.id, rd.restaurant_name;
   
WITH fifty_order AS (
    SELECT
        *
    FROM
        orders_data
    WHERE
        total_order >= 50
)

SELECT
    *, restaurant_id
FROM
    fifty_order fo
JOIN restaurants_data ON restaurants_data.id = fo.id  

# Restaurant dengan rata2 order termahal dengan cte

-- CTE creation
WITH order_result AS (
    SELECT
        restaurant_name,
        (total_order * income_persentage) AS income_result
    FROM
        restaurants_data rd
    JOIN orders_data od ON rd.id = od.restaurant_id
)

-- Final SELECT
SELECT * FROM order_result


# Restaurant dengan rata2 order termahal dengan subquery

 
