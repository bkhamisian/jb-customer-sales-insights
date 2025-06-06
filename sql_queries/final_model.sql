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

     deduplicate as (
         select *
         from duplicate_transactions
         where row_num = 1
         order by customer, processed_date
     ),

    rm_false_transactions as (
        select *
        from deduplicate
        where not (
            discount_id is null
                and discount_in_usd > 0
                and amount_in_usd = 0
                and customer_status = 'existing customer: renewal'
                and license_type = 'Renew'
            )
    ),

     same_day_anomalies as (
         -- Get the full details of the original transactions of anomalies with 'existing customer: new product use'
         select
             rmft.*
         from (select customer,
                      processed_date,
                      product_code
               from rm_false_transactions
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
                rm_false_transactions rmft
                  on rmft.customer = sda.customer
                      and rmft.processed_date = sda.processed_date
                      and rmft.product_code = sda.product_code
         where
             rmft.license_type = 'New'
           and rmft.customer_status in ('new customer', 'existing customer: new product use')
     ),

    rm_same_day_anomalies as (
        select rmft.*
        from rm_false_transactions rmft
        where not exists (
            select 1
            from same_day_anomalies sda
            where rmft.customer = sda.customer
                and rmft.customer_status = 'existing customer: new product use'
            )
    ),

     ranked_purchases as (
         select -- for each customer and each unique product, rank their purchases chronologically.
                customer,
                product_code,
                customer_status,
                license_type,
                processed_date,
                row_number() over (partition by customer, product_code order by processed_date, customer_status) as purchase_rank
         from
             rm_same_day_anomalies
     ),

    same_product_purchase as (
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
        ),

    -- To capture anomalies that this is not the first time the customer has purchased this product with customer_status = "existing customer: new product use"
     rm_same_product_purchase as (
        select rmsda.*
        from rm_same_day_anomalies rmsda
        where not exists (
            select 1
            from same_product_purchase spp
            where rmsda.customer = spp.customer
              and rmsda.processed_date = spp.processed_date
              and rmsda.customer_status = 'existing customer: new product use'
        )
    ),

     fix_license_type as (
        select
            *,
            case
                when (customer_status, license_type) in (
                                                         ('new customer', 'Renew'),
                                                         ('new customer', 'Upgrade'),
                                                         ('existing customer: new product use', 'Renew')
                    )
                    then 'New'
                else license_type
                end as updated_license_type
        from rm_same_product_purchase
    ),

     first_transaction as (
         select
             customer,
             min(processed_date) as first_transaction_date
         from fix_license_type
         where customer_status = 'new customer'
         group by customer
     ),

     invalid_first_entries as (
         select
             t.customer,
             t.processed_date
         from fix_license_type t
                  join first_transaction f
                       on t.customer = f.customer
                           and t.processed_date = f.first_transaction_date
         where t.customer_status = 'existing customer: renewal'
     ),

     clean_dataset as (
        select
            customer,
            customer_status,
            product_code,
            quantity,
            updated_license_type,
            processed_date,
            discount_id,
            amount_in_usd                                                   as paid_amount_in_usd,
            case when discount_in_usd < 0 then 0 else discount_in_usd end   as updated_discount_in_usd,
            case when discount_in_usd < 0 then -discount_in_usd end         as surcharge_amount_in_usd
        from fix_license_type flt
        where not exists (
            select 1
            from invalid_first_entries inv
            where flt.customer = inv.customer
              and flt.processed_date = inv.processed_date
              and flt.customer_status = 'existing customer: renewal'
            )
    ),

     customer_journey as (
         -- For each customer transaction history, find the product code of their previous purchase.
         select
             *,
             lag(product_code) over (partition by customer order by processed_date, customer_status, product_code) as previous_product
         from
             clean_dataset
         order by processed_date
     ),

     paths as (
         -- Define the 'From' and 'To' points for each transition path.
         -- I only care about path destinations: new customer's first purchase or an existing customer buying new product.
         select
             *,
             case
                 -- If there is no previous product, the journey starts from '[New Customer]'.
                 when customer_status = 'new customer' or (previous_product is null and customer_status = 'new customer') then '[New Customer]'
                 else previous_product
                 end as from_path,
             product_code as to_path
         from
             customer_journey
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
               from_path,
               to_path,
               case when customer_status in ('new customer', 'existing customer: new product use')
                             and from_path is not null -- This condition excludes cases where a product might transition to itself (e.g., A -> A)
                             and from_path != to_path -- and makes sure we have a valid starting point for the path.
                   then true else false end as transition_flag,
               round(paid_amount_in_usd + updated_discount_in_usd, 2) as total_amount_in_usd,
               round((updated_discount_in_usd / (paid_amount_in_usd + updated_discount_in_usd) * 100), 0) as discount_percentage
        from paths
    )

select count(*), count(Distinct customer)
from final;
-- 38512,10448

-- select *
-- from final
-- where discount_id is null and updated_discount_in_usd > 0 and paid_amount_in_usd > 0
-- and customer_status = 'existing customer: renewal';

-- what about if the date is exactly the same while calculating the transition?