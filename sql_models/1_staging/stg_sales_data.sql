-- This model cleans the provided source dataset from data anomalies
-- It prepares the data to be used in our marts models

with deduplicate_transactions as ( -- Removing exact duplicate transactions. This will be our base cte
        select *
        from
            (select *,
                    row_number() over (partition by customer, customer_status, product_code, license_type, processed_date, discount_id, quantity, cast(amount_in_usd as string), cast(discount_in_usd as string)) as row_num
            from jetbrains.test_task)
        where row_num = 1
),

     first_transaction as ( -- Getting first transaction for each New customer
        select customer,
               min(processed_date) as first_transaction_date
        from deduplicate_transactions
        where customer_status = 'new customer'
        group by customer
     ),

     invalid_first_entries as ( -- List all customers who are attributed as new customer and renewal on the same day
        select t.customer,
               t.processed_date
        from deduplicate_transactions t
                 inner join first_transaction f
                            on t.customer = f.customer
                                and t.processed_date = f.first_transaction_date
        where t.customer_status = 'existing customer: renewal'
     ),

     new_existing_anomalies as ( -- A customer cannot be both new and existing customer (new product use) at the exact same day for the same product. This will get the list of initial transactions of such customers
        select *
        from
             (select customer,
                     processed_date,
                     product_code,
                     customer_status,
                     count(distinct customer_status) over (partition by customer, processed_date, product_code) as distinct_status_count
              from deduplicate_transactions
              where license_type = 'New' -- Interested only in New licenses
                and customer_status in ('new customer',
                                        'existing customer: new product use'))
        where distinct_status_count > 1 -- filtering for the groups that have more than one distinct status
     ),

     ranked_purchases as ( -- Ranking purchases for each customer and each unique product
        select customer,
               product_code,
               customer_status,
               license_type,
               processed_date,
               discount_id,
               amount_in_usd,
               row_number() over (partition by customer, product_code
                   order by processed_date) as purchase_rank
        from deduplicate_transactions
     ),

     same_product_purchase as ( -- Getting the New purchases for same product twice
        select customer,
               product_code,
               processed_date,
               customer_status,
               license_type,
               purchase_rank
        from ranked_purchases
        where customer_status = 'existing customer: new product use'
          and license_type in ('New') -- The license type should be New
          and purchase_rank > 1 -- To select only records where this is NOT the first time the customer has purchased this product
          and discount_id is null
     ),

     clean_dataset as (
        select *,
               round(paid_amount_in_usd + updated_discount_in_usd, 2) as total_amount_in_usd,
               round((updated_discount_in_usd / (paid_amount_in_usd + updated_discount_in_usd) * 100), 0) as discount_percentage
        from (select customer,
                     customer_status,
                     product_code,
                     quantity,
                     case
                         when (customer_status, license_type) in
                              (('new customer', 'Renew'), ('new customer', 'Upgrade'),
                               ('existing customer: new product use', 'Renew'))
                             then 'New'
                         else license_type
                         end       as updated_license_type,
                     processed_date,
                     discount_id,
                     amount_in_usd as paid_amount_in_usd,
                     case
                         when discount_in_usd < 0 then 0
                         else discount_in_usd
                         end       as updated_discount_in_usd,
                     coalesce(case
                         when discount_in_usd < 0 then -discount_in_usd
                         end, 0)       as surcharge_amount_in_usd
              from deduplicate_transactions t
              where not (
                  discount_id is null -- Removing fully discounted transactions that are coming from existing customers that renewed their license for the upcoming month/year
                      and discount_in_usd > 0
                      and amount_in_usd = 0
                      and customer_status = 'existing customer: renewal'
                      and license_type = 'Renew')
                and not exists -- Removing the records with customer_status = 'existing customer: new product use' and keep 'new customer' as source of truth
                  (select 1
                   from new_existing_anomalies nea
                   where t.customer = nea.customer
                     and t.processed_date = nea.processed_date
                     and t.product_code = nea.product_code
                     and t.customer_status = 'existing customer: new product use')
                and not exists
                  (select 1
                   from same_product_purchase spp -- Removing transactions that show this is not the first time the customer has purchased this product with customer_status = "existing customer: new product use"
                   where t.customer = spp.customer
                     and t.processed_date = spp.processed_date
                     and t.product_code = spp.product_code
                     and t.customer_status = 'existing customer: new product use')
                and not exists
                  (select 1
                   from invalid_first_entries inv -- Removing cases where customer is new and renewal on the same day. Keep only 'new customer' as source of truth
                   where t.customer = inv.customer
                     and t.processed_date = inv.processed_date
                     and t.customer_status = 'existing customer: renewal'))
     )

select customer,
       customer_status,
       product_code,
       quantity,
       updated_license_type,
       processed_date,
       discount_id,
       paid_amount_in_usd,
       updated_discount_in_usd,
       surcharge_amount_in_usd,
       total_amount_in_usd,
       discount_percentage
from clean_dataset
