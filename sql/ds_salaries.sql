ALTER TABLE ds_salaries RENAME COLUMN Column1 TO employee_id

SELECT * FROM ds_salaries ds 

-- Check null data
SELECT * FROM ds_salaries
	WHERE employee_id IS NULL
	OR work_year IS NULL
	OR experience_level IS NULL 
	OR employment_type IS NULL 
	OR job_title IS NULL 
	OR salary IS NULL 
	OR salary_currency IS NULL 
	OR salary_in_usd IS NULL 
	OR employee_residence IS NULL 
	OR remote_ratio IS NULL 
	OR company_location IS NULL 
	OR company_size IS NULL
	
-- List job title
SELECT
	job_title,
	COUNT(job_title)
FROM
	ds_salaries ds
GROUP BY
	job_title
	
-- Job title that has data analyst on it
SELECT
	DISTINCT job_title
FROM
	ds_salaries ds
WHERE
	job_title LIKE '%Data Analyst%'
	

-- Avg salary data analyst
SELECT (AVG(salary) * 15000) / 12 as monthly_salary_idr FROM ds_salaries ds 

-- Avg salary data analyst based on experience and employment type
SELECT
	experience_level,
	employment_type, 
	AVG(salary_in_usd)
FROM
	ds_salaries ds
GROUP BY
	experience_level,
	employment_type
ORDER BY
	experience_level,
	employment_type  
	
-- Country with good salary for da position
SELECT
	company_location,
	AVG(salary_in_usd) as avg_salary_usd
FROM
	ds_salaries
WHERE
	job_title LIKE '%data analyst%'
	AND employment_type = 'FT'
	AND experience_level IN ('MI', 'EN')
GROUP BY
	company_location
HAVING
	avg_salary_usd >= 20000
	
-- In what year there's a highest salary increase from mid to se for DA job and FT
SELECT DISTINCT work_year FROM ds_salaries

-- query for MI
SELECT
	work_year, 
	AVG(salary_in_usd)
FROM
	ds_salaries ds
WHERE
	job_title LIKE '%data analyst%'
	AND employment_type = 'FT'
	AND experience_level = 'MI'
GROUP BY
	work_year 
	
-- query for SE
SELECT
	work_year, 
	AVG(salary_in_usd)
FROM
	ds_salaries ds
WHERE
	job_title LIKE '%data analyst%'
	AND employment_type = 'FT'
	AND experience_level = 'SE'
GROUP BY
	work_year 
	
-- Using CTE for find solution
WITH ex_data AS (
SELECT
	work_year, 
	AVG(salary_in_usd) as avg_ex_salary
FROM
	ds_salaries ds
WHERE
	job_title LIKE '%data analyst%'
	AND employment_type = 'FT'
	AND experience_level = 'EX'
GROUP BY
	work_year 
), 
 mi_data AS (
SELECT
	work_year, 
	AVG(salary_in_usd) as avg_mi_salary
FROM
	ds_salaries ds
WHERE
	job_title LIKE '%data analyst%'
	AND employment_type = 'FT'
	AND experience_level = 'MI'
GROUP BY
	work_year 
 ),
 year_diff AS (
SELECT
	DISTINCT work_year
FROM
	ds_salaries ds)

-- Call it
SELECT
	year_diff.work_year,
    ex_data.avg_ex_salary,
    mi_data.avg_mi_salary,
    ex_data.avg_ex_salary - mi_data.avg_mi_salary as differences
FROM
    year_diff
LEFT JOIN ex_data ON
    year_diff.work_year = ex_data.work_year
LEFT JOIN mi_data ON
    year_diff.work_year = mi_data.work_year
 
-- subquery
SELECT
	yd.work_year,
    mi.avg_mi_salary,
    se.avg_se_salary,
    mi.avg_mi_salary - se.avg_se_salary as differences
FROM
    (
        SELECT
            DISTINCT work_year
        FROM
            ds_salaries
    ) AS yd
LEFT JOIN
    (
        SELECT
            work_year,
            AVG(salary_in_usd) AS avg_mi_salary
        FROM
            ds_salaries
        WHERE
            job_title LIKE '%data analyst%'
            AND employment_type = 'FT'
            AND experience_level = 'MI'
        GROUP BY
            work_year
    ) AS mi ON yd.work_year = mi.work_year
LEFT JOIN
    (
        SELECT
            work_year,
            AVG(salary_in_usd) AS avg_se_salary
        FROM
            ds_salaries
        WHERE
            job_title LIKE '%data analyst%'
            AND employment_type = 'FT'
            AND experience_level = 'SE'
        GROUP BY
            work_year
    ) AS se ON yd.work_year = se.work_year
    

 


	

