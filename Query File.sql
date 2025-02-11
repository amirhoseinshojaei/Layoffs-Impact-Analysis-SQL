-- Project Roadmap

/** 
Remove Duplicates
This step involves identifying and removing duplicate rows from the dataset. Typically, we keep only the first occurrence of each record (e.g., based on company name and layoff date) and remove subsequent duplicate entries to ensure the data is unique.

Standardize the Data
In this step, we standardize the data. This means converting text-based values (such as company names or layoff reasons) to a consistent format. For example, we may convert all company names to uppercase or ensure that all dates follow the same format (e.g., YYYY-MM-DD) for uniformity and ease of use.

Handle Null or Blank Values
This step identifies and addresses missing (NULL) or blank values. We may replace NULL values with a placeholder like "Unknown" or "N/A", or, if critical data is missing (e.g., layoff date), we can remove the rows with these missing values.

Remove Irrelevant or Redundant Columns
This step involves removing any columns that are not necessary for analysis or are redundant. By doing this, we streamline the dataset, keeping only the relevant information for the analysis.
**/

USE world_layoffs;

SELECT *
FROM
	layoffs
;

-- Determining the number of records in a table = 2361
SELECT 
	COUNT(*)
FROM
	layoffs
;


CREATE TABLE layoffs_staging
LIKE layoffs
;


SELECT *
FROM
	layoffs_staging
;


INSERT INTO layoffs_staging
SELECT *
FROM
	layoffs
;


SELECT *,
	ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,
    `date`, stage, country, funds_raised_millions) as row_num
FROM
	layoffs_staging
;


WITH CTE_Table 
AS
(
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,
    `date`, stage, country, funds_raised_millions) as row_num
FROM
	layoffs_staging
)
SELECT *
FROM
	CTE_Table 
WHERE
	row_num > 1
;


SELECT *
FROM
	layoffs_staging
WHERE
	company = 'Casper'
;


SELECT *
FROM
	layoffs_staging
WHERE
	company = 'Cazoo'
;


SELECT *
FROM
	layoffs_staging
WHERE
	company = 'Hibob'
;

SELECT *
FROM
	layoffs_staging
WHERE
	company = 'Wildlife Studios'
;


SELECT *
FROM
	layoffs_staging
WHERE
	company = 'Yahoo'
;


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_numb` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
;

INSERT INTO layoffs_staging2
SELECT *,
	ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,
    `date`, stage, country, funds_raised_millions) as row_num
FROM
	layoffs_staging
;


SELECT *
FROM
	layoffs_staging2
WHERE
	row_numb > 1
;

DELETE 
FROM
	layoffs_staging2
WHERE
	row_numb > 1
;


-- Step2 : Standardizing

SELECT 
	DISTINCT (company),
    TRIM(company)
FROM
	layoffs_staging2
;

UPDATE layoffs_staging2
SET company = TRIM(company)
;

SELECT *,
	CONCAT(UPPER(SUBSTRING(location,1,1), LOWER(SUBSTRING(location,2)))) AS formatted_location
FROM
	layoffs_staging2
;

SELECT *, 
       CONCAT(
           UPPER(SUBSTRING(location, 1, 1)), 
           LOWER(SUBSTRING(location, 2, CHAR_LENGTH(location) - 1))
       ) AS formatted_location
FROM layoffs_staging2
;

UPDATE layoffs_staging2
SET location =  CONCAT(
           UPPER(SUBSTRING(location, 1, 1)), 
           LOWER(SUBSTRING(location, 2, CHAR_LENGTH(location) - 1))
       )
;

UPDATE layoffs_staging2
SET location = TRIM(location)
;

SELECT 
	DISTINCT(industry)
FROM
	layoffs_staging2
ORDER BY 1 ASC
;


SELECT *
FROM
	layoffs_staging2
WHERE 
	industry LIKE 'Crypto%'
;


SELECT 
	COUNT(*)
FROM
	layoffs_staging2
WHERE 
	industry LIKE 'Crypto'
;

SELECT 
	COUNT(*)
FROM
	layoffs_staging2
WHERE 
	industry LIKE 'Crypto Currency'
;

SELECT 
	COUNT(*)
FROM
	layoffs_staging2
WHERE 
	industry LIKE 'CryptoCurrency'
;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE
	industry LIKE 'Crypto%'
;


UPDATE layoffs_staging2
SET industry = TRIM(industry)
;


SELECT *
FROM
	layoffs_staging2
;


SELECT 
	DISTINCT(stage)
FROM
	layoffs_staging2
;


UPDATE layoffs_staging2
SET stage = TRIM(stage)
;

SELECT 
	DISTINCT(country)
FROM
	layoffs_staging2
ORDER BY 1 ASC
;

SELECT 
	COUNT(*)
FROM
	layoffs_staging
WHERE
	country LIKE 'United States.'
;

UPDATE layoffs_staging2
SET country = 'United States'
WHERE
	country LIKE 'United States%'
;


SELECT 
	DISTINCT(country),
    TRIM(TRAILING '.' FROM country)
FROM
	layoffs_staging2
ORDER BY 1
;

SELECT 
	COUNT(*)
FROM
	layoffs_staging
WHERE
	country LIKE '%.'
;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%'
;

SELECT *
FROM
	layoffs_staging2
WHERE
	country LIKE 'United States.'
;

UPDATE layoffs_staging2
SET country = 'United States'
WHERE
	country = 'United States.'
;


SELECT *,
	STR_TO_DATE(`date`,'%m/%d/%Y')
FROM
	layoffs_staging2
;


UPDATE layoffs_staging2
SET `date`= STR_TO_DATE(`date`, '%m/%d/%Y')
WHERE `date` IS NOT NULL
;

SELECT *
FROM
	layoffs_staging2
;


-- Change data type column date to DATE
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE 
;


SELECT *
FROM
	layoffs_staging2
WHERE
	company = '' OR location = '' OR industry= '' OR total_laid_off= '' OR percentage_laid_off=''
    OR stage='' OR country='' OR funds_raised_millions='' 
;

SELECT *
FROM
	layoffs_staging2
WHERE `date` IS NULL
;

SELECT *
FROM
	layoffs_staging2
WHERE
	company = 'Airbnb'
;

SELECT *
FROM
	layoffs_staging2 AS st1
INNER JOIN
	layoffs_staging2 AS st2
    ON st1.company = st2.company
WHERE
	st1.industry = '' AND NOT st2.industry=''
;

UPDATE layoffs_staging2 AS st1
INNER JOIN
	layoffs_staging2 AS st2
    ON st1.company = st2.company
SET st1.industry = st2.industry
WHERE
	st1.industry = '' AND NOT st2.industry=''
;

SELECT *
FROM
	layoffs_staging2
WHERE
	location =''
;


SELECT *
FROM
	layoffs_staging2 AS st1
INNER JOIN 
	layoffs_staging2 AS st2
    ON st1.company = st2.company
WHERE
	st1.location = '' AND NOT st2.location = ''
;


SELECT *
FROM
	layoffs_staging2 AS st1
INNER JOIN 
	layoffs_staging2 AS st2
    ON st1.company = st2.company
WHERE
	st1.location = '' AND NOT st2.location = ''
;

SELECT *
FROM
	layoffs_staging2 AS st1
INNER JOIN 
	layoffs_staging2 AS st2
    ON st1.company = st2.company
WHERE
	st1.total_laid_off = '' AND NOT st2.total_laid_off = ''
;

SELECT *
FROM
	layoffs_staging2 AS st1
INNER JOIN 
	layoffs_staging2 AS st2
    ON st1.company = st2.company
WHERE
	st1.percentage_laid_off = '' AND NOT st2.percentage_laid_off = ''
;

SELECT *
FROM
	layoffs_staging2 AS st1
INNER JOIN 
	layoffs_staging2 AS st2
    ON st1.company = st2.company
WHERE
	st1.stage = '' AND NOT st2.stage = ''
;

SELECT *
FROM
	layoffs_staging2 AS st1
INNER JOIN 
	layoffs_staging2 AS st2
    ON st1.company = st2.company
WHERE
	st1.country = '' AND NOT st2.country = ''
;

SELECT *
FROM
	layoffs_staging2 AS st1
INNER JOIN 
	layoffs_staging2 AS st2
    ON st1.company = st2.company
WHERE
	st1.funds_raised_millions = '' AND NOT st2.funds_raised_millions = ''
;


-- handle Null Values

SELECT *
FROM
	layoffs_staging2
WHERE
	company IS NULL OR location IS NULL OR industry IS NULL
    OR total_laid_off IS NULL OR percentage_laid_off IS NULL OR `date` IS NULL
    OR stage IS NULL OR country IS NULL OR funds_raised_millions IS NULL
;

SELECT 
	COUNT(*)
FROM
	layoffs_staging2
WHERE
	company IS NULL OR location IS NULL OR industry IS NULL
    OR total_laid_off IS NULL OR percentage_laid_off IS NULL OR `date` IS NULL
    OR stage IS NULL OR country IS NULL OR funds_raised_millions IS NULL
;

SELECT *
FROM
	layoffs_staging2
WHERE
	company IS NULL
;

SELECT *
FROM
	layoffs_staging2
WHERE
	industry IS NULL
;

SELECT *
FROM
	layoffs_staging2
WHERE
	location IS NULL
;

SELECT *
FROM
	layoffs_staging2 AS st1
INNER JOIN 
	layoffs_staging2 AS st2
    ON st1.company = st2.company
WHERE
	st1.industry IS NULL AND st1.industry IS NOT NULL
;

SELECT *
FROM
	layoffs_staging2
WHERE
	total_laid_off IS NULL
;

SELECT 
	COUNT(*)
FROM
	layoffs_staging2
WHERE
	total_laid_off IS NULL
;

SELECT *
FROM
	layoffs_staging2 AS st1
INNER JOIN
	layoffs_staging2 AS st2
    ON st1.company = st2.company
WHERE
	st1.total_laid_off IS NULL AND st2.total_laid_off IS NOT NULL
;

UPDATE layoffs_staging2 AS st1
INNER JOIN
	layoffs_staging2 AS st2
    ON st1.company = st2.company
SET st1.total_laid_off = st2.total_laid_off
WHERE
	st1.total_laid_off IS NULL AND st2.total_laid_off IS NOT NULL
;

SELECT 
	count(*)
FROM
	layoffs_staging2 AS st1
INNER JOIN
	layoffs_staging2 AS st2
    ON st1.company = st2.company
WHERE
	st1.total_laid_off IS NULL AND st2.total_laid_off IS NOT NULL
;

SELECT 
	COUNT(*)
FROM
	layoffs_staging2
WHERE
	total_laid_off IS NULL
;


SELECT 
	COUNT(*)
FROM
	layoffs_staging2
WHERE
	percentage_laid_off IS NULL
;


SELECT *
FROM
	layoffs_staging2 AS st1
INNER JOIN
	layoffs_staging2 AS st2
    ON st1.company = st2.company
    AND st1.`date` = st2.`date`
WHERE
	st1.percentage_laid_off IS NULL AND st2.percentage_laid_off IS NOT NULL
;

UPDATE layoffs_staging2 AS st1
INNER JOIN
	layoffs_staging2 AS st2
    ON st1.company = st2.company
    AND st1.`date` = st2.`date`
SET st1.percentage_laid_off = st2.percentage_laid_off
WHERE
	st1.percentage_laid_off IS NULL AND st2.percentage_laid_off IS NOT NULL
;


SELECT *
FROM
	layoffs_staging2
WHERE
	`date` IS NULL
;

SELECT *
FROM
	layoffs_staging2 AS st1
INNER JOIN
	layoffs_staging2 AS st2
    ON st1.company = st2.company
WHERE
	st1.`date` IS NULL AND st2.`date` IS NOT NULL
;

SELECT *
FROM
	layoffs_staging2 AS st1
INNER JOIN
	layoffs_staging2 AS st2
    ON st1.company = st2.company
WHERE
	st1.stage IS NULL AND st2.stage IS NOT NULL
;

SELECT *
FROM
	layoffs_staging2 AS st1
INNER JOIN
	layoffs_staging2 AS st2
    ON st1.company = st2.company
WHERE
	st1.country IS NULL AND st2.country IS NOT NULL
;

-- this is not correct , correct code is below
SELECT *
FROM
	layoffs_staging2 AS st1
INNER JOIN
	layoffs_staging2 AS st2
    ON st1.company = st2.company
WHERE
	st1.funds_raised_millions IS NULL AND st2.funds_raised_millions IS NOT NULL
;

SELECT *
FROM
	layoffs_staging2
WHERE
	company = 'Bolt'
;


SELECT *
FROM
	layoffs_staging2 AS st1
INNER JOIN
	layoffs_staging2 AS st2
    ON st1.company = st2.company
    AND st1.industry = st2.industry
WHERE
	st1.funds_raised_millions IS NULL AND st2.funds_raised_millions IS NOT NULL
;

UPDATE layoffs_staging2 AS st1
INNER JOIN
	layoffs_staging2 AS st2
    ON st1.company = st2.company
    AND st1.industry = st2.industry
SET st1.funds_raised_millions = st2.funds_raised_millions
WHERE
	st1.funds_raised_millions IS NULL AND st2.funds_raised_millions IS NOT NULL
;

SELECT *
FROM
	layoffs_staging2
WHERE
	total_laid_off IS NULL AND percentage_laid_off IS NULL
;

DELETE FROM layoffs_staging2
WHERE 
	total_laid_off IS NULL AND percentage_laid_off IS NULL
;

WITH CTE_DUPLICATES
AS
(
SELECT *,
	ROW_NUMBER() OVER(PARTITION BY company, location, industry,
    total_laid_off, percentage_laid_off, `date`, stage,
    country, funds_raised_millions) AS rw_num2
FROM
	layoffs_staging2
)
SELECT *
FROM
	CTE_DUPLICATES
WHERE
	rw_num2 > 1
;


CREATE TABLE `layoffs_staging_finally` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` date DEFAULT NULL,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_numb` int DEFAULT NULL,
  `rw_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
;

INSERT INTO layoffs_staging_finally
SELECT *,
	ROW_NUMBER() OVER(PARTITION BY company, location, industry,
    total_laid_off, percentage_laid_off, `date`, stage,
    country, funds_raised_millions) AS rw_num2
FROM
	layoffs_staging2
;

SELECT *
FROM
	layoffs_staging_finally
WHERE
	rw_num > 1
;

DELETE FROM layoffs_staging_finally
WHERE
	rw_num > 1
;

ALTER TABLE layoffs_staging_finally
DROP COLUMN rw_num
;

ALTER TABLE layoffs_staging_finally
DROP COLUMN row_numb
;

SELECT *
FROM
	layoffs_staging_finally
;

-- Update the 'continent' column based on the 'country' value.
-- This query categorizes countries into their respective continents:
-- Asia, North America, Europe, South America, or 'Other' for countries not listed.

SELECT 
	DISTINCT(country)
FROM
	layoffs_staging_finally
ORDER BY 1
;

SELECT 
	country,
    CASE
		WHEN country IN ('India', 'China', 'Japan', 'South Korea', 'Pakistan', 'Hong Kong','China',
        'Indonesia','Israel','Malaysia','Thailand','Turkey','Vietnam') THEN 'Asia'
        WHEN country IN ('Canada','United States','Mexico') THEN 'North America'
        WHEN country IN ('Argentina','Brazil', 'Chile','Colombia' ) THEN 'South America'
        WHEN country IN ('Seychelles', 'Senegal', 'Nigeria','Kenya','Egypt') THEN 'Africa'
        WHEN country IN ('Australia','New Zealand') THEN ' Oceania'
        ELSE 'Europe'
	end as continent
FROM 
	layoffs_staging_finally
;

ALTER TABLE layoffs_staging_finally
ADD COLUMN continent VARCHAR(50)
;

UPDATE layoffs_staging_finally
SET continent =  CASE
		WHEN country IN ('India', 'China', 'Japan', 'South Korea', 'Pakistan', 'Hong Kong','China',
        'Indonesia','Israel','Malaysia','Thailand','Turkey','Vietnam') THEN 'Asia'
        WHEN country IN ('Canada','United States','Mexico') THEN 'North America'
        WHEN country IN ('Argentina','Brazil', 'Chile','Colombia' ) THEN 'South America'
        WHEN country IN ('Seychelles', 'Senegal', 'Nigeria','Kenya','Egypt') THEN 'Africa'
        WHEN country IN ('Australia','New Zealand') THEN ' Oceania'
        ELSE 'Europe'
	end
;

SELECT *
FROM
	layoffs_staging_finally
;