-- DATA CLEANING

SELECT * FROM layoffs

CREATE table layoffs_staging LIKE layoffs

SELECT * FROM layoffs_staging

INSERT INTO layoffs_staging SELECT * FROM layoffs 

SELECT * FROM layoffs_staging

-- 1. Check and remove duplicates

SELECT
	*,
	ROW_NUMBER() OVER(PARTITION BY company,
	location,
	industry,
	total_laid_off,
	percentage_laid_off,
	'date',
	stage,
	country,
	funds_raised_millions) AS row_num
FROM
	layoffs_staging
	
WITH duplicate_cte AS (
SELECT
	*,
	ROW_NUMBER() OVER(
            PARTITION BY company,
	location,
	industry,
	total_laid_off,
	percentage_laid_off,
	'date',
	stage,
	country,
	funds_raised_millions) AS row_num
FROM
	layoffs_staging
)
SELECT
	*
FROM
	duplicate_cte
WHERE
	row_num > 1

SELECT * FROM layoffs_staging ls WHERE company = 'Oracle'

-- Delete rows from the layoffs_staging table
DELETE
FROM
    layoffs_staging
WHERE
    -- Specify the columns that define a duplicate entry
    (company,
    location,
    industry,
    total_laid_off,
    percentage_laid_off,
    date,
    stage,
    country,
    funds_raised_millions) IN (
    
    -- Subquery to identify duplicate rows
    SELECT
        company,
        location,
        industry,
        total_laid_off,
        percentage_laid_off,
        date,
        stage,
        country,
        funds_raised_millions
    FROM
        (
        -- Inner subquery that assigns a row number to each row within each group of duplicates
        SELECT
            company,
            location,
            industry,
            total_laid_off,
            percentage_laid_off,
            date,
            stage,
            country,
            funds_raised_millions,
            ROW_NUMBER() OVER(
                PARTITION BY company,
                location,
                industry,
                total_laid_off,
                percentage_laid_off,
                date,
                stage,
                country,
                funds_raised_millions
            ) AS row_num
        FROM
            layoffs_staging
        ) AS ranked
    -- Filter to include only rows where row_num is greater than 1 (i.e., duplicates)
    WHERE
        row_num > 1
);

-- We can also make staging2

CREATE TABLE `layoffs_staging2` (
  `company` varchar(50) DEFAULT NULL,
  `location` varchar(50) DEFAULT NULL,
  `industry` varchar(50) DEFAULT NULL,
  `total_laid_off` varchar(50) DEFAULT NULL,
  `percentage_laid_off` varchar(50) DEFAULT NULL,
  `date` varchar(50) DEFAULT NULL,
  `stage` varchar(50) DEFAULT NULL,
  `country` varchar(50) DEFAULT NULL,
  `funds_raised_millions` varchar(50) DEFAULT NULL,
  `row_num` int default null
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

SELECT * FROM layoffs_staging2

INSERT INTO layoffs_staging2 SELECT
	*,
	ROW_NUMBER() OVER(PARTITION BY company,
	location,
	industry,
	total_laid_off,
	percentage_laid_off,
	'date',
	stage,
	country,
	funds_raised_millions) AS row_num
FROM
	layoffs_staging

SELECT * FROM layoffs_staging2 WHERE row_num > 1

SELECT * FROM layoffs_staging2 WHERE company = 'Yahoo'

DELETE FROM layoffs_staging2 WHERE row_num > 1

SELECT * FROM layoffs_staging2 ls 

ALTER table layoffs_staging2 DROP COLUMN row_num

SELECT * FROM layoffs_staging2 ls 

-- 2. Standardize the data

SELECT company, TRIM(company) FROM layoffs_staging2

UPDATE layoffs_staging2 SET company = TRIM(company) 

SELECT company from layoffs_staging2 ls 

SELECT DISTINCT industry FROM layoffs_staging2 ls ORDER BY 1

SELECT * FROM layoffs_staging2 ls WHERE industry LIKE 'Crypto%'

UPDATE layoffs_staging2 
SET industry = 'Crypto' WHERE industry LIKE 'Crypto%'

SELECT DISTINCT country FROM layoffs_staging2 ORDER BY 1

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country) FROM layoffs_staging2 ORDER BY 1

UPDATE
	layoffs_staging2
SET
	country = TRIM(TRAILING '.' FROM country)
WHERE
	country LIKE 'United States%'

SELECT DISTINCT country FROM layoffs_staging2 ls

SELECT `date` FROM layoffs_staging2 ls 

SELECT `date`, STR_TO_DATE(`date`, '%m/%d/%Y') AS converted_date FROM layoffs_staging2

SELECT `date`, DATE_FORMAT(STR_TO_DATE(`date`, '%m/%d/%Y'), '%d/%m/%Y') AS formatted_date FROM layoffs_staging2

UPDATE layoffs_staging2 SET date = NULL WHERE date = ' '

SELECT date FROM layoffs_staging2 WHERE date IS NULL

SELECT * FROM layoffs_staging2 ls WHERE company = 'Blackbaud'

SELECT COUNT(*) FROM layoffs_staging2 WHERE `date` IS NULL

SELECT `date`, 
       CASE 
           WHEN `date` LIKE '%/%/%' THEN 'MM/DD/YYYY or similar'
           WHEN `date` LIKE '%-%-%' THEN 'YYYY-MM-DD or similar'
           ELSE 'Unknown Format'
       END as format_detected
FROM layoffs_staging2
  
UPDATE layoffs_staging2 SET `date` = DATE_FORMAT(STR_TO_DATE(`date`, '%Y-%m-%d'), '%d/%m/%Y')
WHERE `date` LIKE '%-%-%' AND STR_TO_DATE(`date`, '%Y-%m-%d') IS NOT NULL

SELECT * FROM layoffs_staging2 ls 

ALTER TABLE layoffs_staging2 MODIFY COLUMN `date` DATE

ALTER TABLE layoffs_staging2 ADD COLUMN temp_date DATE

UPDATE layoffs_staging2 SET temp_date = STR_TO_DATE(`date`, '%d/%m/%Y')
WHERE `date` LIKE '%/%/%' AND STR_TO_DATE(`date`, '%d/%m/%Y') IS NOT NULL

SELECT `date`, temp_date FROM layoffs_staging2

ALTER TABLE layoffs_staging2 MODIFY COLUMN `temp_date` DATE AFTER `date`

SELECT
	*
FROM
	layoffs_staging2 ls 

-- 3. Handle with null / blank values if any
	
SELECT * FROM layoffs_staging2 ls WHERE total_laid_off IS NULL 

SELECT * FROM layoffs_staging2 ls WHERE total_laid_off = 'NULL' OR total_laid_off IS NULL

UPDATE layoffs_staging2 SET total_laid_off = NULL WHERE total_laid_off = 'NULL'

SELECT * FROM layoffs_staging2 ls WHERE total_laid_off IS NULL 

SELECT * FROM layoffs_staging2 ls WHERE percentage_laid_off = 'NULL'

UPDATE layoffs_staging2 SET percentage_laid_off = NULL WHERE percentage_laid_off = 'NULL'

SELECT * FROM layoffs_staging2 ls WHERE percentage_laid_off IS NULL 

SELECT * FROM layoffs_staging2 ls

SELECT * FROM layoffs_staging2 ls WHERE funds_raised_millions = 'NULL'

UPDATE layoffs_staging2 SET funds_raised_millions = NULL WHERE funds_raised_millions = 'NULL'

SELECT * FROM layoffs_staging2 ls WHERE industry = 'NULL'

SELECT * FROM layoffs_staging2 ls WHERE industry = ''

UPDATE layoffs_staging2 SET industry = NULL WHERE industry = 'NULL' 

UPDATE layoffs_staging2 SET industry = NULL WHERE industry = '' 

SELECT * FROM layoffs_staging2 ls WHERE industry IS NULL 

SELECT * FROM layoffs_staging2 ls JOIN layoffs_staging2 ls2
	ON ls.company = ls2.company 
	WHERE ls.industry IS NULL AND ls2.industry IS NOT NULL 
		
SELECT ls.company, ls.industry, ls2.industry
FROM layoffs_staging2 ls
JOIN layoffs_staging2 ls2 ON ls.company = ls2.company
WHERE ls.industry IS NULL AND ls2.industry IS NOT NULL

UPDATE layoffs_staging2 ls JOIN layoffs_staging2 ls2 
	ON ls.company = ls2.company
	SET ls.industry = ls2.industry
	WHERE ls.industry IS NULL AND ls2.industry IS NOT NULL
	
SELECT * FROM layoffs_staging2 ls
-- 4. Remove irrelevant columns

SELECT * FROM layoffs_staging2 ls 
	WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL

DELETE FROM layoffs_staging2 ls 
	WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL
	
SELECT * FROM layoffs_staging2 ls 