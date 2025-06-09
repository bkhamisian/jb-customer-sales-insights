-- Data types
select column_name,
       data_type
from jetbrains.INFORMATION_SCHEMA.COLUMNS
where table_name = 'test_task';

----------------------------------------------------------------------------------
-- Total records - Unique customers
select count(*)                 as total_transactions,
       count(distinct customer) as total_unique_customers
from jetbrains.test_task;

-- Many customers appear more than once (up to 10+ times). Due to multiple purchases, renewals or upgrades
-- Steadily decreasing after 4+ transactions
select cnt_transactions,
       count(*)             as cnt_customers
from (
    select customer,
           count(*)  as cnt_transactions
    from jetbrains.test_task
    group by 1
    )
group by 1
order by cnt_transactions;

----------------------------------------------------------------------------------
-- Two groups: New and Existing (renewal, new product use, additional new licenses)
-- Should be useful when analyzing customer segmentation

select
    customer_status,
    count(*)                 as total_records,
    count(distinct customer) as total_unique_customers
from jetbrains.test_task
group by 1;

----------------------------------------------------------------------------------
-- 13 different products
select product_code,
       count(*)                 as total_transactions,
       count(distinct customer) as total_unique_customers
from jetbrains.test_task
group by 1;

----------------------------------------------------------------------------------
-- Only one record with quantity 2. What does it mean?
select quantity,
       count(*)                 as total_transactions,
       count(distinct customer) as total_unique_customers
from jetbrains.test_task
group by 1;

----------------------------------------------------------------------------------
-- Three distinct license types: New, Renew and Upgrade
-- When analyzing product lifecycle (New -> upgrade -> Renew)
select
    license_type,
    count(*)                 as total_records,
    count(distinct customer) as unique_customers
from jetbrains.test_task
group by 1;

----------------------------------------------------------------------------------
-- Date range from 2018-01-01 to 2020-12-31 - complete 3 years of data
select min(processed_date)              as min_date,
       max(processed_date)              as max_date,
       count(distinct processed_date)   as distinct_days
from jetbrains.test_task;

----------------------------------------------------------------------------------
-- 115 unique discount codes.
select count(distinct discount_id)
from jetbrains.test_task;

-- 2514 transactions with discount_id filled.
select discount_id is null as is_null_discount_id,
       count(*)            as total_transactions
from jetbrains.test_task
group by 1;

----------------------------------------------------------------------------------
-- minimum amount = 0
-- maximum amount = 675.98
select min(amount_in_usd) as min_amount,
       max(amount_in_usd) as max_amount
from jetbrains.test_task;

-- We have 1,404 cases were discount_amount >= amount_in_usd when discount existed (includes 100% discounts).
-- This implies that the amount_in_usd is net (after discount has been applied).
select  *
from jetbrains.test_task
where discount_in_usd >= amount_in_usd
  and discount_id is not null;

----------------------------------------------------------------------------------
-- 104 Negative discounts (discount_id is null). Needs inspection.
-- 14039 records where discount amount is 0 and the discount_id field is null. This is a correct scenario.
-- 2514 cases where discount > 0 and discount_id is not null. This a correct scenario.
-- 22772 cases where discount > 0 and discount_id being null. Big problem here.

select
    case when discount_in_usd < 0 then 'negative'
            when discount_in_usd = 0 then 'zero'
            when discount_in_usd > 0 then 'positive'
            else null end                              as discount_amount_flag,
    discount_id is null                                as is_null_discount,
    count(*)                                           as total_transactions,
    count(distinct customer)                           as total_unique_customers
from jetbrains.test_task
group by
    discount_amount_flag,
    discount_id is null;

----------------------------------------------------------------------------------

