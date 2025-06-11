-- This model gets all the necessary fields from the cleaned dataset (stg_sales_data)
-- It then creates a monthly summary of key subscription metrics (MRR, Active Customers, ...) by dividing yearly subscriptions into monthly values
-- This model is used to build the Subscription Revenue Health dashboard

-- The cases that I am unable to define the billing interval, I will assume these are yearly because in most sas businesses annual plans are the standard
-- Also, choosing Monthly in such cases is not so correct as it would overcalculate the MRR for a yearly plan by 12 factor
with recursive cleaned_dataset as (
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
         from jetbrains.stg_sales_data
),

     transaction_history as (
         select *,
                lag(processed_date) over (partition by customer, product_code order by processed_date) as previous_date,
                lag(total_amount_in_usd) over (partition by customer, product_code order by processed_date) as previous_total_amount,
                lead(processed_date) over (partition by customer, product_code order by processed_date) as next_date,
                lead(total_amount_in_usd) over (partition by customer, product_code order by processed_date) as next_total_amount,
                case
                    when date_diff(processed_date, lag(processed_date) over (partition by customer, product_code
                        order by processed_date), day) between 25 and 45 then 'Monthly'
                    when date_diff(processed_date, lag(processed_date) over (partition by customer, product_code
                        order by processed_date), day) > 320 then 'Yearly'
                    end as backward_interval,
                case
                    when date_diff(lead(processed_date) over (partition by customer, product_code
                        order by processed_date), processed_date, day) between 25 and 45 then 'Monthly'
                    when date_diff(lead(processed_date) over (partition by customer, product_code
                        order by processed_date), processed_date, day) > 320 then 'Yearly'
                    end as forward_interval
         from cleaned_dataset
        ),

    classified_interval_transactions as ( -- Classifying all transactions as Monthly or Yearly
         select customer,
                product_code,
                customer_status,
                updated_license_type,
                processed_date,
                paid_amount_in_usd,
                updated_discount_in_usd,
                total_amount_in_usd,
                date_diff(processed_date, previous_date, day) as date_diff_1,
                backward_interval,
                forward_interval,
                case
                    -- #1: 'Upgrade' case for a "new product use". Prioritizing forward looking since new product don't have previous transaction
                    when updated_license_type = 'Upgrade' -- p.updated_license_type = 'Upgrade'
                        and customer_status = 'existing customer: new product use'
                        and
                         total_amount_in_usd between (next_total_amount * 0.85) and (next_total_amount * 1.15)
                        then coalesce(forward_interval, 'Yearly')

                    -- #2: For existing customer: renewal -> prioritize the backward looking interval since we'll have previous transaction date
                    when updated_license_type = 'Upgrade' -- p.updated_license_type = 'Upgrade'
                        and customer_status = 'existing customer: renewal'
                        and
                         total_amount_in_usd between (previous_total_amount * 0.85) and (previous_total_amount * 1.15)
                        then coalesce(backward_interval, 'Yearly')

                    -- 3: Additional new licenses means they had subscribed to the product before case. Prioritizing backward looking
                    when customer_status = 'existing customer: additional new licenses'
                        then coalesce(backward_interval, 'Yearly')

                    -- #4: The new customer case. Must look forward since no historical data exists
                    when customer_status = 'new customer' then coalesce(forward_interval, 'Yearly')

                    -- #5: extreme cases
                    when backward_interval is not null then backward_interval

                    -- #6: extreme cases
                    when forward_interval is not null then forward_interval

                    else 'Yearly' -- If a transaction is completely out of the logic above or if it's a single entry -> Yearly
                    end                                           as final_billing_interval
         from transaction_history
        ),

    division_of_yearly as ( -- Using recursion to expand only the Yearly subscriptions into 12 monthly rows
        select customer,
               updated_license_type,
               date_trunc(processed_date, month) as mrr_month,
               paid_amount_in_usd / 12 as monthly_revenue,
               1 as month_counter
        from classified_interval_transactions
        where final_billing_interval = 'Yearly'
        union all -- This is the recursive part where it will generate the next 11 months for yearly subscriptions
        select customer,
               updated_license_type,
               date_add(mrr_month, INTERVAL 1 month) as mrr_month,
               monthly_revenue,
               month_counter + 1
        from division_of_yearly
        where month_counter < 12
        ),

    all_combined_data as ( -- Combining the expanded yearly data with the original monthly data
        select customer,
               updated_license_type,
               mrr_month,
               monthly_revenue
        from division_of_yearly
        union all
        select customer,
               updated_license_type,
               date_trunc(processed_date, month) as mrr_month,
               paid_amount_in_usd as monthly_revenue
        from classified_interval_transactions
        where final_billing_interval = 'Monthly'
    ),

    monthly_summary as ( -- Creating the customer level monthly summary model
        select mrr_month,
               customer,
               (min(mrr_month) over (partition by customer)) = mrr_month as is_new_customer_this_month,
               sum(monthly_revenue) as total_mrr -- Simplified to total MRR per customer
        from all_combined_data
        group by mrr_month,
                 customer
    ),

    final_calculation as (
        select mrr_month,
               customer,
               is_new_customer_this_month,
               total_mrr,
               lag(total_mrr) over (partition by customer order by mrr_month) as previous_customer_mrr
        from monthly_summary
    ),

    monthly_aggregation as (
        select mrr_month,
               round(sum(total_mrr), 2)                                       as mrr,
               count(distinct customer)                                       as active_customers,
               round(sum(total_mrr) / nullif(count(distinct customer), 0), 2) as arpu,
               round(sum(case
                             when is_new_customer_this_month then total_mrr
                             else 0
                   end), 2)                                                   as new_mrr, -- Total MRR from customers in their first month
               round(sum(case
                             when not is_new_customer_this_month and total_mrr > previous_customer_mrr then total_mrr - previous_customer_mrr
                             else 0
                   end), 2)                                                   as expansion_mrr, -- Any MRR increase from existing customers
               round(sum(case
                             when total_mrr < previous_customer_mrr then previous_customer_mrr - total_mrr
                             else 0
                   end), 2)                                                   as contraction_mrr, -- Any MRR decrease from existing customers (who have not churned)
--                round(sum(case
--                              when total_mrr = 0 and previous_customer_mrr > 0 then previous_customer_mrr
--                              else 0
--                    end), 2)                                                   as churned_mrr -- The previous month's MRR for customers who have 0 MRR this month
               count(distinct case when not is_new_customer_this_month then customer end) as existing_customers
        from final_calculation
        group by mrr_month
    ),

    final as (
        select *,
               lag(mrr) over (order by mrr_month) as previous_mrr,
               lag(active_customers) over (order by mrr_month) as previous_month_active_customers,
               -- Churn MRR will not be calculated like this to make sure the waterfall chart is perfectly visualized
               -- I approached it like this because the source data makes it difficult to directly identify churned customers
               -- Even though my initial calculation provided accurate metrics (close to the below), this approach make sre the chart is visualized correctly
               round((lag(mrr) over (order by mrr_month) + new_mrr + expansion_mrr - contraction_mrr) - mrr, 2) as churned_mrr,
               ((lag(active_customers) over (order by mrr_month)) - existing_customers) as churned_customers
        from monthly_aggregation
    )

select
    mrr_month,
    mrr,
    previous_mrr,
    new_mrr,
    expansion_mrr,
    contraction_mrr,
    churned_mrr,
    active_customers,
    arpu,
    existing_customers,
    churned_customers
from final
