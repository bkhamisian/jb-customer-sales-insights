-- Found exact duplicate transactions. Deduplicate them.
with duplicate_transactions as (
    select *,
           row_number() over (
               partition by
                   customer,
                   customer_status,
                   product_code,
                   license_type,
                   processed_date,
                   discount_id,
                   quantity,
                   cast(amount_in_usd as string),
                   cast(discount_in_usd as string)
               ) as row_num
    from jetbrains.test_task
),

-- select *
-- from numbered_transactions
-- where row_num > 1;

     deduplicate as (
          select *
          from duplicate_transactions
          where row_num = 1
          order by customer, processed_date
     )

select * --count(*), count(distinct customer)
from deduplicate;
-- where customer = 3025561;

----------------------------------------------------------------------------------
-- Some campaigns had 0$ revenue (2,615 transaction)
-- 1,099 transactions are tracked by discount code. The rest have discount amount > 0
select
    coalesce(cast(discount_id as string), 'untracked_discount')     as grouped_discount_id,
    count(*)                                                        as cnt_transactions,
    round(avg(discount_in_usd), 2)                                  as avg_discount,
    round(avg(amount_in_usd), 2)                                    as avg_revenue,
    round(sum(discount_in_usd), 2)                                  as total_discount,
    round(sum(amount_in_usd), 2)                                    as total_revenue,
    round(sum(amount_in_usd) / nullif(sum(discount_in_usd), 0), 2)  as effective_discount_roi
from jetbrains.test_task
where
    amount_in_usd = 0
group by grouped_discount_id
order by grouped_discount_id desc;

----------------------------------------------------------------------------------
-- 909 transactions are the existing customers that renewed their licenses for the upcoming month/year.
select *
from jetbrains.test_task
where discount_id is null
  and discount_in_usd > 0
  and amount_in_usd = 0
  and customer_status  = 'existing customer: renewal' and license_type = 'Renew';


-- Decided to remove these cases. Add in the final query
select *
from jetbrains.test_task
where not (
    discount_id is null
        and discount_in_usd > 0
        and amount_in_usd = 0
        and customer_status = 'existing customer: renewal'
        and license_type = 'Renew'
    );
----------------------------------------------------------------------------------
-- customer_status, license_type
select
    customer_status,
    license_type,
    count(*)                 as total_records,
    count(distinct customer) as total_unique_customers
from jetbrains.test_task
group by 1, 2;

-- Data quality checks for the 3 suspicious cases (6 records)
select *
from jetbrains.test_task
where (customer_status, license_type)
          in (
              ('new customer','Renew'),
              ('existing customer: new product use','Renew'),
              ('new customer','Upgrade')
          );

-- Understanding ('existing customer: new product use','Renew') and ('new customer','Upgrade') by looking at similar transactions
with customer_status_flags as (
    select
        customer,
        processed_date,
        max(case when customer_status = 'new customer' then 1 else 0 end) as has_new_customer,
        max(case when customer_status = 'existing customer: new product use' then 1 else 0 end) as has_new_product_use
    from `jetbrains.test_task`
    where customer_status IN ('new customer', 'existing customer: new product use')
    group by customer, processed_date
)
select customer, processed_date
from customer_status_flags
where has_new_customer = 1 and has_new_product_use = 1
order by customer, processed_date;
----------------------------------------------------------------------------------
-- Validating that each customer_id starts with customer_status = 'new customer'
-- Except the customers that their first transaction is before 2018 because is not included in this dataset
with first_transaction as (
    select
        customer,
        min(processed_date) as first_transaction_date
    from jetbrains.test_task
    where customer_status = 'new customer'
    group by customer
),
     first_status as (
         select
             t.customer,
             t.processed_date,
             t.customer_status,
             t.license_type
         from jetbrains.test_task t
                  join first_transaction f
                       on t.customer = f.customer
                           and t.processed_date = f.first_transaction_date
     )

select *
from first_status
where customer_status  != 'new customer';
----------------------------------------------------------------------------------
-- Study 104 negative records
select customer_status, count(*)
from jetbrains.test_task
where discount_in_usd < 0
group by 1;

select *
from jetbrains.test_task
where customer = 3440643;

----------------------------------------------------------------------------------
-- Studying transactions when discount > 0 and discount_id being null
select amount_in_usd = 0 as flag, count(*)
from jetbrains.test_task
where discount_in_usd > 0 and discount_id is null
group by flag;

select  license_type, count(*)
from jetbrains.test_task
where discount_id is null and discount_in_usd > 0 and amount_in_usd = 0
  and customer_status = 'existing customer: new product use'
group by 1;

----------------------------------------------------------------------------------
-- Studying the transition of products (from_path = to_path / cases where from_path is null / New customer cases )
with customer_journey as (
         select
             *,
             lag(product_code) over (partition by customer order by processed_date, customer_status desc, product_code) as previous_product
         from
             jetbrains.test_task
         order by processed_date
     ),

     paths as (
         select
             *,
             case
                 when previous_product is null and customer_status = 'new customer' then '[New Customer]'
                 else previous_product
                 end as from_path,
             product_code as to_path
         from
             customer_journey
     )

select customer_status, license_type,/* from_path, to_path,*/ count(*) as cnt_transactions
from paths
where from_path = to_path
group by 1,2;

-- I want to capture the same day anomalies. That is having "new customer" and "existing customer: new product use" both on the same day
select -- Get the full details of the original transactions of anomalies with 'existing customer: new product use'
         tt.*
         from (select customer,
                      processed_date,
                      product_code
               from jetbrains.test_task
               where license_type = 'New'                                   -- Interested only in 'New' licenses
                 and customer_status in
                     ('new customer', 'existing customer: new product use') -- with these two customer status categories
               group by customer,
                        processed_date,
                        product_code
               having
                   -- Looking for groups that contain both distinct statuses.
                   count(distinct customer_status) = 2) sda
         inner join
                jetbrains.test_task tt
                  on tt.customer = sda.customer
                      and tt.processed_date = sda.processed_date
                      and tt.product_code = sda.product_code
         where
               tt.license_type = 'New'
           and tt.customer_status in ('new customer', 'existing customer: new product use');

-- To capture anomalies that this is not the first time the customer has purchased this product with customer_status = "existing customer: new product use"
with ranked_purchases as (
    select -- for each customer and each unique product, rank their purchases chronologically.
        customer,
        product_code,
        customer_status,
        license_type,
        processed_date,
        row_number() over (partition by customer, product_code order by processed_date, customer_status) as purchase_rank
    from
        jetbrains.test_task
)

select
    customer,
    product_code,
    processed_date,
    customer_status,
    license_type,
    purchase_rank
from
    ranked_purchases
where
    customer_status = 'existing customer: new product use'   -- The status is 'new product use'.
  and license_type = 'New' -- The license type is 'New'.
  and purchase_rank > 1 -- To capture anomalies
order by
    customer,
    processed_date;

----------------------------------------------------------------------------------
