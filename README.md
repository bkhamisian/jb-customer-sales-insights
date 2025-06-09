# Sales Data Analysis

### Project by: Baret Khamisian

***

## 1. Project Objective and Overview

This project analyzes a transactional sales dataset _(data/test_task.csv)_. The primary goal is to explore the data to uncover insights into product performance, discount effectiveness, customer behavior, and the overall health of the subscription business model. 

The final output is a combination of four dashboards designed to provide clear, actionable insights for internal stakeholders:
* **Dashboard 1: Executive Overview** - This dashboard provides an overview of top performing products, overall revenue trends, and the company's product business positioning.
* **Dashboard 2: Discount Effectiveness** - This dashboard measures the financial impact and effectiveness of discounts by analyzing revenue per sale, customer acquisition cost and optimal discount levels.
* **Dashboard 3: Customer Transition Paths** - This dashboard visualizes the customer journey. The most popular paths users take when moving between different products or upgrading within the same product line.
* **Dashboard 4: Subscription Revenue Health** - This dashboard measures the core financial health of the subscription business through key metrics like Monthly Recurring Revenue (MRR), Average Revenue Per User (ARPU) and Customer Churn.

## 2. Tools & Technologies
* **Data Storage:** Google BigQuery
* **Querying and Model Development:** IntelliJ IDEA
* **BI & Visualization Tool _(For both EDA and Dashboard)_:** Tableau Desktop

## 3. How to Review This Project

This repository is structured to provide a clear overview of the entire process.

**Dashboards**: The complete set of dashboards including all charts and insights, is available in this repository _(dashboards/..........)_. Additionally, they are published on Tableau Public for easy access:
* **[Link to your final Tableau Public Dashboard]**

#### **SQL Models**
All SQL code used for data cleaning, eda, transformation, and creating the final data models can be found in the `sql_models/` directory.

```
sql_models/
|-- analysis/            # Exploratory and sanity queries
|   |-- 0_sanity_checks.sql
|   |-- 1_exploratory_data_analysis.sql
|
|-- marts/               # Final, clean data models for Tableau
|   |-- 2_mart_transactional_analysis.sql    # For dashboards: Executive Overview, Discount Effectiveness and Customer Transition Paths
|   |-- 3_mart_subscription_metrics.sql      # For dashboard: Subscription Revenue Health
```

#### **Presentation & Detailed Documentation**
For a more detailed walkthrough of the project, including the presentation slides and in depth documentation:
* **Google Slides Presentation:** `[Link to your Google Slides]`
* **Google Doc (Full Sanity Checks, EDA, key findings and the assumptions made to clean the data - basically my thought process on how I approached the task from beginning to end):** `[Link to your Google Doc]`
