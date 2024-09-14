-- Data Cleaning

select * from layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or blank values
-- 4. Remove any columns


-- 1. Remove Duplicates
-- create new table
create table layoff_staging
like layoffs;

insert layoff_staging
select * from layoffs;

select * from layoff_staging;

with duplicate_cte as 
(
select *,
row_number() over (
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoff_staging
)
select * 
from duplicate_cte 
where row_num > 1;

with duplicate_cte as 
(
select *,
row_number() over (
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoff_staging
)
delete
from duplicate_cte 
where row_num > 1;

CREATE TABLE `layoff_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoff_staging2
select *,
row_number() over (
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoff_staging;

select * from layoff_staging2 where row_num > 1;
delete from layoff_staging2 where row_num > 1;

-- 2. Standardize the Data
select * from layoff_staging2;
select company, Trim(company) from layoff_staging2;

update layoff_staging2
set company = Trim(company);

select industry, count(distinct(industry)) 
from layoff_staging2 
group by industry
order by industry;

select * from layoff_staging2 where industry like 'Crypt%';

update layoff_staging2
set industry = 'Crypto'
where industry like 'Crypt%';

select distinct(industry) from layoff_staging2 order by industry;

select distinct(country) 
from layoff_staging2 
where country like 'United Sta%'
order by country;

update layoff_staging2
set country = Trim(trailing '.' from country)
where country like 'United Sta%';

select distinct(country) 
from layoff_staging2 
where country like 'United Sta%'
order by country;


select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoff_staging2;

update layoff_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

select `date`
from layoff_staging2;

alter table layoff_staging2
modify column `date` date;


-- 3. Null Values or blank values

select * from layoff_staging2
where total_laid_off is null 
and percentage_laid_off  is null;

select * from layoff_staging2
where industry is null or industry = '';

select * from layoff_staging2
where company like 'Carvana%';


-- join table to populate the missing values

update layoff_staging2
set industry = null
where industry = '';

select t1.company,t1.industry ,  t2.industry
from layoff_staging2 t1
join layoff_staging2 t2
	on t1.company = t2.company
    and t1.location = t2.location
where t1.industry is null or t1.industry = ''
and t2.industry is not null;


update layoff_staging2 t1
join layoff_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null 
and t2.industry is not null;

-- 4. Remove any columns and rows
select * from layoff_staging2
where total_laid_off is null 
and percentage_laid_off  is null;

delete 
from layoff_staging2
where total_laid_off is null 
and percentage_laid_off  is null;

select * from layoff_staging2;

alter table layoff_staging2
drop column row_num;


-- Exploratory Data Analysis (EDA)
select max(total_laid_off), max(percentage_laid_off)
from layoff_staging2;

select * 
from layoff_staging2
where percentage_laid_off > 0.5
order by percentage_laid_off desc;

select company, sum(total_laid_off) sum_total
from layoff_staging2
group by company
order by sum_total  desc;

select min(`date`), max(`date`)
from layoff_staging2;

select industry, sum(total_laid_off) sum_total
from layoff_staging2
group by industry 
order by sum_total desc;

select country, sum(total_laid_off) sum_total
from layoff_staging2
group by country
order by sum_total desc;

select country, sum(percentage_laid_off) sum_percentage
from layoff_staging2
group by country
order by sum_percentage desc;


select year(`date`), sum(total_laid_off) sum_total
from layoff_staging2
group by year(`date`)
order by year(`date`) desc;

select month(`date`), sum(total_laid_off) sum_total
from layoff_staging2
group by month(`date`)
order by sum_total desc ;

select substring(`date`, 1,7) as `month`,sum(total_laid_off) sum_total
from layoff_staging2
where `date` is not null
group by `month`
order by sum_total desc;

-- rolling total with cte

with rolling_total as 
(
select substring(`date`, 1,7) as `month`,sum(total_laid_off) sum_total
from layoff_staging2
where `date` is not null
group by `month`
order by `month` 
)
select `month`, sum_total, sum(sum_total) over (order by `month`) as total_rolling
from rolling_total;

select company, year(`date`),sum(total_laid_off) sum_total
from layoff_staging2
where year(`date`) is not null
and total_laid_off is not null
group by company, year(`date`)
order by sum_total desc;

create or replace view company_year as
select company, year(`date`) as date_year ,sum(total_laid_off) sum_total
from layoff_staging2
where year(`date`) is not null
and total_laid_off is not null
group by company, year(`date`)
order by sum_total desc;

create table company_ranking as
select *,
dense_rank() over(partition by date_year order by sum_total desc) as ranking 
from company_year
order by ranking asc;

select * 
from company_ranking
where ranking <= 5
order by date_year;
