-- EDA layoffs

SELECT * FROM layoffs_staging2

SELECT MAX(total_laid_off), MAX(percentage_laid_off) FROM layoffs_staging2

ALTER TABLE layoffs_staging2 MODIFY COLUMN total_laid_off INT NULL

SELECT MAX(total_laid_off), MAX(percentage_laid_off) FROM layoffs_staging2

SELECT * FROM layoffs_staging2 WHERE percentage_laid_off = 1 ORDER BY total_laid_off DESC 

SELECT company, SUM(total_laid_off) FROM layoffs_staging2 GROUP BY company ORDER BY 2 DESC

SELECT MIN(temp_date), MAX(temp_date) FROM layoffs_staging2

SELECT industry, SUM(total_laid_off) FROM layoffs_staging2 GROUP BY industry ORDER BY 2 DESC

SELECT country , SUM(total_laid_off) FROM layoffs_staging2 GROUP BY country ORDER BY 2 DESC

SELECT temp_date , SUM(total_laid_off) FROM layoffs_staging2 GROUP BY temp_date ORDER BY 2 DESC

SELECT YEAR(temp_date) , SUM(total_laid_off) FROM layoffs_staging2 GROUP BY YEAR(temp_date) ORDER BY 1 DESC

SELECT stage , SUM(total_laid_off) FROM layoffs_staging2 GROUP BY stage ORDER BY 2 DESC

SELECT
	SUBSTR(temp_date, 6, 2) as 'Month',
	SUM(total_laid_off)
FROM
	layoffs_staging2
GROUP BY
	SUBSTR(temp_date, 6, 2)

SELECT
    SUBSTR(temp_date, 1, 7) as 'Month',
    SUM(total_laid_off)
FROM
    layoffs_staging2
WHERE
    SUBSTR(temp_date, 1, 7) IS NOT NULL
GROUP BY
    SUBSTR(temp_date, 1, 7) 
ORDER BY 1 ASC

WITH Rolling_Total as
(
    SELECT
        SUBSTR(temp_date, 1, 7) as Month,
        SUM(total_laid_off) as total_off
    FROM
        layoffs_staging2
    WHERE
        SUBSTR(temp_date, 1, 7) IS NOT NULL
    GROUP BY
        Month
)
SELECT 
    Month, 
    SUM(total_off) OVER(ORDER BY Month) as rolling_total
FROM 
    Rolling_Total;
    




