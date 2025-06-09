-- This model cleans the provided dataset and calculates key metrics including product transitions.
-- It prepares the data for use in three dashboards: Executive Summary, Discount Effectiveness and Customer Transition Paths.

with deduplicate_transactions as ( -- Removing exact duplicate transactions. This will be our base cte.
        select *
        from
            (select *,
                    row_number() over (partition by customer, customer_status, product_code, license_type, processed_date, discount_id, quantity, cast(amount_in_usd as string), cast(discount_in_usd as string)) as row_num
             from jetbrains.test_task)
        where row_num = 1
     ),

     same_day_anomalies as ( -- A customer cannot be both new and existing customer at the exact same day. Get the list of transactions of such customers.
        select rmft.*
        from
            (select customer,
                    processed_date,
                    product_code
             from deduplicate_transactions
             where license_type = 'New' -- Interested only in New licenses
               and customer_status in ('new customer',
                                       'existing customer: new product use')
             group by customer,
                      processed_date,
                      product_code
             having count(distinct customer_status) = 2) as sda -- Looking for groups that contain both distinct statuses.
                inner join deduplicate_transactions rmft
                    on rmft.customer = sda.customer
                    and rmft.processed_date = sda.processed_date
                    and rmft.product_code = sda.product_code
        where rmft.license_type = 'New'
          and rmft.customer_status in ('new customer',
                                       'existing customer: new product use')
     ),

     ranked_purchases as ( -- Ranking purchases for each customer and each unique product.
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
          and license_type in ('New') -- The license type should be New.
          and purchase_rank > 1 -- To select only records where this is NOT the first time the customer has purchased this product.
          and discount_id is null
     ),

     first_transaction as (
        select customer,
               min(processed_date) as first_transaction_date
        from deduplicate_transactions
        where customer_status = 'new customer'
        group by customer
     ),

     invalid_first_entries as ( -- Getting customers who are attributed as new customer and renewal on the same day.
        select t.customer,
               t.processed_date
        from deduplicate_transactions t
        inner join first_transaction f
            on t.customer = f.customer
            and t.processed_date = f.first_transaction_date
        where t.customer_status = 'existing customer: renewal'
     ),

     clean_dataset as (
        select customer,
               customer_status,
               product_code,
               quantity,
               case
                   when (customer_status,license_type) in
                        (('new customer','Renew'), ('new customer','Upgrade'), ('existing customer: new product use','Renew'))
                       then 'New'
                   else license_type
                   end as updated_license_type,
               processed_date,
               discount_id,
               amount_in_usd as paid_amount_in_usd,
               case
                   when discount_in_usd < 0 then 0
                   else discount_in_usd
                   end as updated_discount_in_usd,
               case
                   when discount_in_usd < 0 then -discount_in_usd
                   end as surcharge_amount_in_usd
        from deduplicate_transactions t
        where not (
            discount_id is null -- Removing fully discounted transactions that are coming from existing customers that renewed their license (Renew license type) for the upcoming month/year
                and discount_in_usd > 0
                and amount_in_usd = 0
                and customer_status = 'existing customer: renewal'
                and license_type = 'Renew')
          and not exists -- Removing the records with customer_status = 'existing customer: new product use' and keep 'new customer' as source of truth
              (select 1
               from same_day_anomalies sda
               where t.customer = sda.customer
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
                   and t.customer_status = 'existing customer: renewal')
     ),

     customer_journey as ( -- For each customer transaction history, find the product code of their previous purchase
        select *,
               lag(product_code) over (partition by customer
                   order by processed_date, customer_status, product_code) as previous_product
        from clean_dataset
        order by processed_date
     ),

     paths as ( -- Defining the From and To points for each transition path. I only care about path destinations to -> new customers first purchase or an existing customer buying new product
        select *,
               case
                   when customer_status = 'new customer'
                       or (previous_product is null
                           and customer_status = 'new customer') then '[New Customer]' -- If there is no previous product and customer is a new customer, then the journey starts from '[New Customer]'
                   else previous_product
                   end as from_path,
               product_code as to_path
        from customer_journey
     ),

     final as (
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
               case
                   when from_path != to_path
                       and transition_flag is false
                       and from_path is not null then product_code
                   else from_path
                   end as from_path, -- The from_path of the renewed product matches the original product in case that product has been bought before. A -> A, B -> B (not A -> B)
               to_path,
               transition_flag,
               round(paid_amount_in_usd + updated_discount_in_usd, 2) as total_amount_in_usd,
               round((updated_discount_in_usd / (paid_amount_in_usd + updated_discount_in_usd) * 100), 0) as discount_percentage
        from
            (select *,
                    case
                        when customer_status in ('new customer',
                                                   'existing customer: new product use')
                              and from_path is not null
                              and from_path != to_path -- This condition excludes cases where a product might transition to itself and makes sure we have a valid starting point for the path
                              then true
                          else false
                          end as transition_flag
               from paths)
     )

select *
from final
