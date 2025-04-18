
-- Data Cleaning Section --

-- Create Staging Table from Original Data --
CREATE TABLE layoffs_staging LIKE layoffs;
INSERT layoffs_staging 
SELECT * FROM layoffs;


-- Add Row Numbers to Identify Duplicates
SELECT *, 
ROW_NUMBER() 
OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Identify Duplicate Records using CTE
WITH duplicate_cte AS (
  SELECT *, 
  ROW_NUMBER() 
  OVER(
  PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
  FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte 
WHERE row_num > 1;


-- Create Second Staging Table with Row Numbers
CREATE TABLE layoffs_staging2 (
  `company` TEXT,
  `location` TEXT,
  `industry` TEXT,
  `total_laid_off` INT DEFAULT NULL,
  `percentage_laid_off` TEXT,
  `date` TEXT,
  `stage` TEXT,
  `country` TEXT,
  `funds_raised_millions` INT DEFAULT NULL,
  `row_num` INT
);

-- Insert Data with Row Numbers into Staging2 Table
INSERT INTO layoffs_staging2
SELECT *, 
ROW_NUMBER() 
OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
FROM layoffs_staging;

-- Remove Duplicate Records from Staging2
DELETE 
FROM layoffs_staging2 
WHERE row_num > 1;

-- Clean Company Names by Trimming Extra Spaces
UPDATE layoffs_staging2 
SET company = TRIM(company);

-- Standardize Industry Names (e.g., Crypto)
UPDATE layoffs_staging2 
SET industry = 'Crypto' 
WHERE industry LIKE 'Crypto%';

-- Clean Country Names by Removing Periods
UPDATE layoffs_staging2 
SET country = TRIM(TRAILING '.' FROM country) 
WHERE country LIKE 'United States%';

-- Convert Date Column to Proper Date Format
UPDATE layoffs_staging2 
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');
ALTER TABLE layoffs_staging2 
MODIFY COLUMN `date` DATE;

-- Remove Empty Industry Entries by Setting Them to NULL
UPDATE layoffs_staging2 
SET industry = NULL 
WHERE industry = '';

-- Fill Missing Industry Values by Matching Companies
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2 ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;

-- Remove Rows with Null Layoff Data
DELETE FROM layoffs_staging2 
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

-- Drop Helper Column (row_num) After Cleaning
ALTER TABLE layoffs_staging2 
DROP COLUMN row_num;



-- Exploratory Data Analysis (EDA) --


-- View Cleaned Data
SELECT * 
FROM layoffs_staging2;

-- Maximum Layoff Count and Percentage
SELECT MAX(total_laid_off), 
MAX(percentage_laid_off) 
FROM layoffs_staging2;

-- Companies with 100% Layoffs
SELECT * 
FROM layoffs_staging2 
WHERE percentage_laid_off = 1 
ORDER BY total_laid_off DESC;

-- Top Companies with 100% Layoffs and High Funding
SELECT * 
FROM layoffs_staging2 
WHERE percentage_laid_off = 1 
ORDER BY funds_raised_millions DESC;

-- Top Companies by Total Layoffs
SELECT company, 
SUM(total_laid_off) AS total 
FROM layoffs_staging2 
GROUP BY company 
ORDER BY total DESC;

-- Date Range of Layoff Data
SELECT MIN(`date`) AS start_date, 
MAX(`date`) AS end_date 
FROM layoffs_staging2;


-- Grouped Insights --

-- Layoffs by Industry
SELECT industry, 
SUM(total_laid_off) AS total 
FROM layoffs_staging2 
GROUP BY industry 
ORDER BY total DESC;

-- Layoffs by Country
SELECT country, 
SUM(total_laid_off) AS total 
FROM layoffs_staging2 
GROUP BY country 
ORDER BY total DESC;

-- Layoffs by Stage of Funding
SELECT stage, 
SUM(total_laid_off) AS total 
FROM layoffs_staging2 
GROUP BY stage 
ORDER BY total DESC;

-- Sum of Percentage Laid Off by Company
SELECT company, 
SUM(percentage_laid_off) AS total_percent 
FROM layoffs_staging2 
GROUP BY company 
ORDER BY total_percent DESC;

-- Average Percentage Laid Off by Company
SELECT company, 
AVG(percentage_laid_off) AS avg_percent 
FROM layoffs_staging2 
GROUP BY company 
ORDER BY avg_percent DESC;


-- Time-Based Analysis --

-- Layoffs by Date
SELECT `date`, 
SUM(total_laid_off) AS total 
FROM layoffs_staging2 
GROUP BY `date` 
ORDER BY total DESC;

-- Layoffs by Year
SELECT YEAR(`date`) AS years, 
SUM(total_laid_off) AS total 
FROM layoffs_staging2 
GROUP BY years
ORDER BY years DESC;

-- Monthly Layoffs Summary
SELECT SUBSTRING(`date`,1,7) AS `month`, 
SUM(total_laid_off) AS total
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `month`
ORDER BY `month` ASC;

-- Monthly Rolling Total of Layoffs
WITH Rolling_Total AS (
  SELECT SUBSTRING(`date`,1,7) AS `month`, 
  SUM(total_laid_off) AS total
  FROM layoffs_staging2
  WHERE SUBSTRING(`date`,1,7) IS NOT NULL
  GROUP BY `month`
)
SELECT `month`, total,
       SUM(total) OVER(ORDER BY `month`) AS rolling_total
FROM Rolling_Total;


