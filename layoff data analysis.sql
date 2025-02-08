# data cleaning
SELECT *
FROM LAYOFFS;

-- 1. REMOVING DUPLICATED
-- 2. STANDARDIZE THE DATA
-- 3. FIXING NULL VALUES OR BLANK VALUES
-- 4. REMOVE ANY IRRELEVANT COLUMNS

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY Company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
 FROM layoffs_staging;
 
 WITH duplicate_cte AS
 (SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY Company,location, industry, total_laid_off, percentage_laid_off, `date`,
country, funds_raised_millions) as row_num
 FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;

SELECT * FROM
layoffs_staging
WHERE company = 'casper';

WITH duplicate_cte AS
 (SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY Company,location, industry, total_laid_off, percentage_laid_off, `date`,
country, funds_raised_millions) as row_num
 FROM layoffs_staging
)
Delete  
FROM duplicate_cte
WHERE row_num > 1;

CREATE TABLE `layoffs_staging2` (
	`company` text,
	`location` text,
	`industry` text,
	`total_laid_off` int default NULL,
	`percentage_laid_off` text,
	`date` text,
	`stage` text,
	`country` text,
	`funds_raised_millions` int default NULL,
	`row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



SELECT * FROM layoffs_staging2
where row_num>1;

INSERT INTO layoffs_staging2
SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY Company,location, industry, total_laid_off, percentage_laid_off, `date`,
country, funds_raised_millions) as row_num
 FROM layoffs_staging;
 
 
DELETE
FROM layoffs_staging2
where row_num>1;


SELECT * FROM layoffs_staging2;

-- STANDARDIZING DATA

SELECT company , Trim(company)
from layoffs_staging2;

UPDATE layoffs_staging2
SET company = Trim(company);

SELECT *
from layoffs_staging2
Where industry like 'Crypto%' ;

UPDATE layoffs_staging2
SET Industry = 'crypto'
Where industry like 'Crypto%' ;


SELECT DISTINCT country, TRIM(TRAILING '.' FROM  country)
from layoffs_staging2
order by 1;

UPDATE  layoffs_staging2
SET country = TRIM(TRAILING '.' FROM  country)
Where country LIKE 'united States%';

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y') 
FROM layoffs_staging2;


UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date` 
FROM layoffs_staging2;

ALTER table layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2
where total_laid_off is NULL AND
percentage_laid_off is NULL ;

UPDATE layoffs_staging2
SET industry = NULL
where industry = '';

SELECT *
from layoffs_staging2
where industry is NULL OR industry = '' ;

SELECT *
from layoffs_staging2
where company = 'airbnb';

select * 
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
where (t1.industry is NULL OR t1.industry = '')
AND t2.industry is not null;
 

UPDATE layoffs_staging2 t1
JOIN layoffs_staging t2
	on t1.company = t2.company
SET t1.industry = t2.industry
where t1.industry is NULL
AND t2.industry is not null;

UPDATE layoffs_staging2
SET industry = NULL
where industry = '';

SELECT *
from layoffs_staging2
where company like 'bally%';

SELECT *
from layoffs_staging2;

SELECT *
FROM layoffs_staging2
where total_laid_off is NULL AND
percentage_laid_off is NULL ;


Delete
FROM layoffs_staging2
where total_laid_off is NULL AND
percentage_laid_off is NULL ;

SELECT *
from layoffs_staging2;

ALTER Table layoffs_staging2
DROP COLUMn row_num;

-- EXPLORATORY DATA ANALYSIS

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROm layoffs_staging2;

SELECT *
from layoffs_staging2
WHERE percentage_laid_off = 1;

SELECT company , SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT MIN(`date`) , MAX(`date`)
FROM layoffs_staging2;

SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT *
from layoffs_staging2;

SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY  stage
ORDER BY 2 DESC;

SELECT SUBSTRING(`date`, 1, 7) As `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) is NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;


WITH Rolling_total AS
(
SELECT SUBSTRING(`date`, 1, 7) As `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) is NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH` ,total_off,
SUM(total_off) OVER(ORDER By `MONTH`) AS rolling_total
FROM Rolling_total;


SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;


SELECT company,YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY  company,YEAR(`date`)
ORDER BY 3 DESC;


WITH company_year (compnay , years, total_laid_off) AS
(
SELECT company,YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY  company,YEAR(`date`)
ORDER BY 3 DESC
), Company_year_rank AS
(
SELECT * , 
DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM company_year
WHERE years is NOT NULL
)
SELECT * 
FROM Company_year_rank
WHERE Ranking <= 5;




