-- This model gets all the necessary fields and metrics from the cleaned dataset (stg_sales_data) and calculates the customer product transition
-- It prepares the data for use in three dashboards: Executive Overview, Discount Effectiveness and Customer Transition Paths

with customer_journey as ( -- For each customer transaction history, find the product code of their previous purchase
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
               discount_percentage,
               lag(product_code) over (partition by customer
                   order by processed_date, customer_status, product_code) as previous_product
        from jetbrains.stg_sales_data
        order by processed_date
     ),

     paths as ( -- Defining the From and To points for each transition path. I only care about path destinations to -> new customers first purchase or an existing customer buying new product
        select *,
               case
                   when customer_status = 'new customer'
                       or (previous_product is null
                           and customer_status = 'new customer') then '[New Customer]' -- If there is no previous product and customer is a new customer, then the journey starts from [New Customer]
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
               total_amount_in_usd,
               discount_percentage,
               case
                   when from_path != to_path
                       and transition_flag is false
                       and from_path is not null then product_code
                   else from_path
                   end as from_path, -- The from_path of the renewed product matches the original product in case that product has been bought before. A -> A, B -> B (not A -> B)
               to_path,
               transition_flag
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
