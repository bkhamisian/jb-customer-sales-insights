-- customer_status, license_type
select
    customer_status,
    license_type,
    count(*)                 as total_records,
    count(distinct customer) as total_unique_customers
from jetbrains.test_task
group by 1, 2;

-- Data quality checks for the 3 cases (6 records)
select *
from jetbrains.test_task
where (customer_status, license_type)
          in (
              ('new customer','Renew'),
              ('existing customer: new product use','Renew'),
              ('new customer','Upgrade')
    );

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

