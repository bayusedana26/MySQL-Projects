SELECT * FROM meals_data 

SELECT * FROM restaurants_data rd 

SELECT restaurant_name, restaurant_type_id FROM restaurants_data

SELECT
	restaurant_id,
	meal_type_id,
	restaurant_name
FROM
	meals_data
JOIN restaurants_data ON
	meals_data.id = restaurants_data.id
	
SELECT
    hot_cold, meal_name,
    CONCAT(hot_cold, ' ', meal_name) AS concatenated,
    CONCAT_WS(' ', hot_cold, meal_name) AS capitalized_concat,
    CONCAT_WS('/', 'www.instagram.com', LOWER(hot_cold)) AS ig_url,
    SUBSTRING(CONCAT_WS('/', 'www.instagram.com', LOWER(hot_cold)), 19, 50) AS ig_username,
    REPEAT(meal_name, 10) AS meal_duplicate,
    REVERSE(meal_name) AS reversed,
    TRIM(meal_name) AS clean_meal_name,
    LPAD(meal_name, 10, hot_cold) AS lpad_result,
    RPAD(meal_name, 10, hot_cold) AS rpad_result,
    REPLACE(CONCAT_WS('/', 'www.instagram.com', LOWER(hot_cold), 'hot_cold', 'tiktok'), 'old_string', 'new_string') AS tiktok_url
FROM
    meals_data

		
UPDATE meals_data
	SET meal_name = 'Meal 1'
	WHERE meal_name = '      Meal 1'
	

	

