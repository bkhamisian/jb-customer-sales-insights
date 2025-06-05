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
        from rm_false_transactions
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

    final as (
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
--             discount_in_usd,
            case when discount_in_usd < 0 then -discount_in_usd end         as surcharge_amount_in_usd
        from fix_license_type flt
        where not exists (
            select 1
            from invalid_first_entries inv
            where flt.customer = inv.customer
              and flt.processed_date = inv.processed_date
              and flt.customer_status = 'existing customer: renewal'
            )
    )

select *,
       paid_amount_in_usd + updated_discount_in_usd as total_amount_in_usd
--     count(*), count(distinct customer)
from final;
-- 38512,10448

-- select *
-- from final
-- where discount_id is null and updated_discount_in_usd > 0 and paid_amount_in_usd > 0
-- and customer_status = 'existing customer: renewal';



