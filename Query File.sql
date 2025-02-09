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


