# Sales Data Analysis

### Project by: Baret Khamisian

***

## 1. Project Objective and Overview

This project analyzes a transactional sales dataset _(data/test_task.csv)_. The primary goal is to explore the data to uncover insights into product performance, discount effectiveness, customer behavior, and the overall health of the subscription business model. 

The final output is a combination of _**four dashboards under two tableau workbooks**_ designed to tell a story and provide clear insights:

* **BaretKhamisian_BI_Task_Dashboard1.twbx:**  A deep dive into transactional sales data, analyzing product performance, discount effectiveness, and key customer transition paths.
    * **Dashboard 1: Executive Overview** - This dashboard provides an overview of top performing products, overall revenue trends, and the companys product positioning through a volume of transactions and discounts.
    * **Dashboard 2: Discount Effectiveness** - This dashboard measures the financial impact and effectiveness of discounts by analyzing revenue per sale, customer acquisition cost and optimal discount levels.
    * **Dashboard 3: Customer Transition Paths** - This dashboard visualizes the customer journey by mapping the most valuable and popular paths users take when moving between products or upgrading.
* **BaretKhamisian_BI_Task_Dashboard2.twbx:**
    * **Dashboard 4: Subscription Revenue Health** - This dashboard measures the financial health of the subscription business through advanced metrics like MRR trend/movement, Net Dollar Retention, Churn Rate and a detailed MRR waterfall analysis.

## 2. Tools & Technologies
* **Data Storage:** Google BigQuery
* **Querying and Model Development:** IntelliJ IDEA
* **BI & Visualization Tool _(For both EDA and Dashboard)_:** Tableau Desktop _(Also published on Tableau Public)_

## 3. How to Review This Project

This repository is structured to provide a clear overview of the entire process.

#### 📊 Dashboards
The complete set of dashboards, including all charts and insights, can be found in the `dashboards` directory of this repository. For easy access and sharing, they are also published on Tableau Public:

- **[BaretKhamisian_BI_Task_Dashboard1](https://public.tableau.com/views/BaretKhamisian_BI_Task_Dashboard1/ExecutiveOverview?:language=en-US&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link)**  

- **[BaretKhamisian_BI_Task_Dashboard2](https://public.tableau.com/views/BaretKhamisian_BI_Task_Dashboard2/SubscriptionRevenueHealth?:language=en-US&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link)**

- **[BaretKhamisian_BI_Task_EDA](https://public.tableau.com/views/BaretKhamisian_BI_Task_EDA/UA-RevenueSplit?:language=en-US&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link)**  

**Note:** For the most reliable experience, it's recommended to download and open the packaged workbooks (`.twbx` files) in **Tableau Desktop**. While Tableau Public offers easy access, certain complex visualizations such as the Sankey diagram (for product transition flow) may be impacted by performance limitations. That said, all key transition paths are also visualized in the **Heatmap** _(in the same dashboard)_. So even if you're using Tableau Public, you won't miss any critical insights.


#### SQL Models:
All SQL code used for data cleaning, eda, transformation, and creating the staging and final marts data models can be found in the `sql_models` directory.

```
sql_models/
|-- 0_analysis/            # Exploratory and sanity queries
|   |-- exploratory_data_analysis.sql
|   |-- sanity_checks.sql
|
|-- 1_staging/            # Staging model that cleans the source dataset
|   |-- stg_sales_data.sql
|
|-- 2_marts/               # Final data models for Tableau (References the staging model)
|   |-- mart_subscription_metrics.sql      # For dashboard: Subscription Revenue Health
|   |-- mart_transactional_analysis.sql    # For dashboards: Executive Overview, Discount Effectiveness and Customer Transition Paths
```

#### Presentation & Detailed Documentation:
For a more detailed walkthrough of the project, including the presentation slides and in depth documentation:
* **Google Slides Presentation: [Google Slides Link](https://docs.google.com/presentation/d/1BH9Bqmi4hNzqeRC4dMUw13xOCCylsXUcQBSuTo-dPWk/edit?usp=sharing)**
* **Google Doc (Full Sanity Checks, EDA, key findings and the assumptions made to clean the data - basically my thought process on how I approached the task from beginning to end): [Google Docs Link](https://docs.google.com/document/d/1yNVsaPHpLSUzWAyLm8Axr-L2FEb-sgP_OUWh18_kx2s/edit?usp=sharingLink)**