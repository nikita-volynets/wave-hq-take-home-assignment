# Task 1

## Task 1: Build a data model to allow business stakeholders to answer their own questions (self-serve)

To complete this exercise, you will need to use the sample repo and sample data provided in the repo: https://github.com/waveaccounting/data-take-home-assignment

Please fork the repo and submit your home work into the forked repo

The README file in the repo provides setup instructions.

**Business Case:** A new ‘Premium’ subscription product was recently launched by a financial services tech company called *Surf* that has historically provided free Accounting and Invoicing products to small businesses in the United States and Canada.

Growing this new ‘Premium’ subscription is a major priority for the organization and the Product team would like to understand more about its subscribers. The team would like to be able to answer the following questions and build the following data visualizations:

- How many active subscriptions do we have on any given historical day?
- How frequently are Premium subscribers churning from the product?
- What portion of revenue is coming from each specific purchase method, at a given point in time?
- Visualize the revenue by month broken down by purchase method.

**Tasks:**

- Assuming this data is raw, and we have no pre-existing dbt models built to support this request, build all of the appropriate dbt sources, models, and tests.
- Applying the Kimball approach would be a significant advantage to your home task assignment results.
- Your models must be able to answer *at least* the questions stated above.
- The reviewer should be able to build your dbt project and select from the models you have developed
- **Please note:** You do not need to build specific queries to answer those questions. We are not looking for specific answers, we are looking for you to build the models that will let someone else (business stakeholders) answer these questions by themselves, using a BI tool.

**Questions you should be prepared to answer about your models:**

- How can you validate that the output of your model is correct?
- How would you go about teaching the Product team to use this modelled data to answer their questions?

You will be asked to present your models in a 15-minute presentation to Data and AI team members with questions to follow.

## Answer

## Tools:

- dbt Core
- Snowflake

## DBT Project Structure

The project will be organized in three main layers using the Kimball dimensional modelling approach:

1. **Staging**:  `stg_business`, `stg_subscription_items` ,`stg_subscriptions` - 1-1 to raw data with minor transformations like renaming fields or changing data types.
- **Intermediate**: `dim_customers` ,`dim_subscription_items` ,`dim_dates`, `fct_subscriptions`, `fct_subscription_daily_snapshots` - dimensional and fact models
- **Marts**: - models with final transformations

## Building models

### 1. Staging Layer

The Staing Layer is the foundation of our project, it will contain individual models that we are going to use on the next layers to build more complex models.

We will build staging models based on the raw tables that we have:

- `raw_business` - info about each business who is subscribed to *Surf*s product
- `raw_subscription_items` - details about the individual subscription plan, including their plan type, billing cycle, and pricing.
- `raw_subscriptions` - info about customer subscriptions, including their business association, purchase details, term periods, and status.

Models on staging layer have 1-1 relationship with raw data with minor transformations:

- Renaming fields
- Converting data types
- Converting field format. For example, conversing Python-like string into separate fields.

```sql
--models/staging/stg_business

WITH source AS (

SELECT *
FROM {{ source('raw', 'raw_business') }}

)

SELECT
    id AS business_id,
    create_date AS business_create_date,
    country AS business_country,
    organizational_type,
    type AS business_type,
    subtype AS business_subtype
FROM source
```

```sql
--models/staging/stg_subscription_items

WITH source AS (

    SELECT *
    FROM {{ source('raw', 'raw_subscription_items') }}
    
)

SELECT
    id as subscription_item_id,
    unit_type,
    billing_period_unit,
    unit_price
FROM source
```

```sql
--models/staging/stg_subscriptions

WITH source AS (

    SELECT *
    FROM {{ source('raw','raw_subscriptions') }}

),

parsed AS (

    SELECT
        id,
        business_id,
        created_at,
        current_term_start,
        current_term_end,
        cancel_schedule_created_at,
        cancelled_at,
        channel,
        country,
        status,
        exchange_rate,
        -- Converting single quotes to double quotes, then parsing as JSON.
        parse_json(
            replace(subscription_plan, '''', '"')
        ) AS plan_json
    FROM source

)

SELECT
    id as subscription_id,
    business_id,
    created_at,
    current_term_start,
    current_term_end,
    cancel_schedule_created_at,
    cancelled_at,
    channel,
    country as subscription_country,
    status as subscription_status,
    exchange_rate,
    plan_json:"item_id"::integer       AS subscription_item_id,
    plan_json:"currency_code"::string AS currency_code

FROM parsed
```

### 2. Intermediate Layer

In our company we use Kimball data modelling, so intermediate layer will consist of fact and dimensional tables.

- **Dimensions -** descriptive data (customers and subscription items info)
- **Facts -** measurable business events (in our case - subscriptions)

**Dimensional tables:**

Dimension tables are used to represent contextual or descriptive information for a business process event.

```sql
--models/intermediate/dim_business

WITH final AS (
    SELECT *
    FROM {{ ref('stg_business') }}
)

SELECT
    business_id,
    business_create_date,
    business_country,
    organizational_type,
    business_type,
    business_subtype
FROM final

```

```sql
--models/intermediate/dim_subscription_items

WITH final AS (

    SELECT *
    FROM {{ ref('stg_subscription_items') }}
)

SELECT
    subscription_item_id,
    unit_type,
    billing_period_unit,
    unit_price
FROM final
```

We will create a `dim_dates` table to make it easier and faster to analyze data over time by storing  date details like months, weeks, years, etc. This model will be created with `dbt_date` package.

```sql
--models/intermediate/dim_dates

{{ dbt_date.get_date_dimension("1990-01-01", "2050-12-31") }}
```

**Fact tables:**

Fact tables are database tables that represent a business process in the real world. `fct_subscriptions` is a Accumulating Snapshot fact table. Each subscripton has several milestone dates that are updated over time and the rows are not added for each state change; instead, they are updated when the process continues.

```sql
--models/intermediate/fct_subscriptions

WITH final AS (
	
	SELECT *
	FROM {{ ref('stg_subscriptions') }}

)

SELECT
    subscription_id,
    business_id,
    created_at,
    current_term_start,
    current_term_end,
    cancel_schedule_created_at,
    cancelled_at,
    channel,
    subscription_country,
    subscription_status,
    exchange_rate,
    subscription_item_id,
    currency_code
FROM final

```

To handle daily active subscription counts, monthly revenue by channel, etc., we create a table that expands each subscription from its `created_date` to either `cancelled_at` (if earlier) or to the `current_date`. Each row corresponds to a single subscription on a single date.

```sql
--models/intermediate/fct_subscription_daily_snapshots

WITH subscription_ranges AS (

SELECT
    s.subscription_id,
    s.business_id,
    cast(s.subscription_created_at AS date) AS subscription_created_on,
    cast(s.current_term_start AS date) AS current_term_start_on,
    cast(s.current_term_end AS date) AS current_term_end_on,
    cast(s.cancelled_at AS date) AS cancelled_date_on,
    cast(s.cancel_schedule_created_at AS date) AS cancel_schedule_date_on,
    s.subscription_status,
    s.channel,
    s.exchange_rate,
    s.currency_code,
    s.subscription_item_id,
    -- Unifying revenue in USD:
    case 
        when si.billing_period_unit = 'month' 
                then si.unit_price * s.exchange_rate 
        when si.billing_period_unit = 'year' 
                then (si.unit_price * s.exchange_rate) / 12
        else si.unit_price * s.exchange_rate
    END as monthly_recurring_revenue
FROM {{ ref('fct_subscriptions') }} s
INNER JOIN {{ ref('dim_subscription_items') }} si
    ON s.subscription_item_id = si.subscription_item_id

),

calendar_days AS (

    SELECT 
        date_day
    FROM {{ ref('dim_dates') }} 
    WHERE date_day BETWEEN '2020-01-01' AND current_date

),

expanded AS (

    SELECT
        sr.subscription_id,
        sr.business_id,
        c.date_day,
        sr.channel,
        sr.monthly_recurring_revenue,
        CASE
            WHEN c.date_day >= sr.cancelled_date_on THEN 'cancelled'
            ELSE 'active'
        END AS second_status
    FROM subscription_ranges sr
    INNER JOIN calendar_days c
        ON c.date_day 
	        BETWEEN sr.subscription_created_on
	        AND coalesce(sr.cancelled_date_on, sr.current_term_end_on, current_date)
)

SELECT * FROM expanded

```

### 3. Marts Layer

The Marts Layer is used for models with final transformations. It builds clean, denormalized structured tables using building blocks from the intermediate layer, making them suitable for reporting and analysis. For this task, our focus is on self-serve analysis, so we won’t build any marts models at this moment.

## Output Validation & Data Quality

1. **Data Contracts**

We can use Data Contracts to verify the outputs of our models. They check if our model output contains the column names, data types, and constraints that we defined in the yml file.

Example:

```yaml
# models/intermediate/fct

version: 2

models:
  - name: dim_business
    description: "Business registration data including business type, subtype, and organizational structure."
    config:
      contract:
        enforced: true
      materialized: table

    columns:
      - name: business_id
        description: "Unique identifier for each business."
        data_type: integer
        constraints:
          - type: primary_key

      - name: business_create_date
        description: "The date when the business was created."
        data_type: date
        constraints:
          - type: not_null
          
    ...
```

2. **Tests**

We can ensure the data quality of our models by utilizing different types of tests. They can check if data in our fields is unique, doesn’t have null values, or perform more advanced logic checks.

For more advanced testing, we can write our own custom tests, use unit tests to validate transformation logic, or leverage packages like `dbt_expectations` for additional validation.

Example of built-in tests:

```yaml
# models/

version: 2

models:
  - name: stg_business
    description: "Staging model for business data, standardizing column names and ensuring data integrity."
    columns:
      - name: business_id
        description: "Unique identifier for the business."
        tests:
          - unique
          - not_null

      - name: business_create_date
        description: "The date when the business was created."
        tests:
          - not_null
          
  ...
```

Examples of failures:

![image](https://github.com/user-attachments/assets/e22ed5fa-07f7-4619-a686-e096f23d1547)

3. **Monitoring & Alerts**

We can validate the outputs with stakeholders to ensure they align with their expectations.

In the BI tool, we can set up alerts for key metrics and receive notifications in Slack when an alert is triggered.

## Teaching Strategy

1. **Understand Business Needs:** clarify the business need and expectations before the training;
2. **Training:** conduct live session with the stakeholders;
3. **Documentation:** create and provide clear up-to-date documentation and how-to guide for the team.
4. **Support:** create slack channel and host office hours. 
5. **Measure Success:** check data usage in a BI tool and gather regular feedback using survey


# Task 2

## Task 2: Refactor this query, explain your refactoring choices and how it improves the query

Submit the refactored dbt model(s) you would propose.

Explain any changes, improvements, comments, or modifications (including adding or removing models) you would make to the project.

What approach would you take to ensure the outputs of your refactored models are accurate?

How would you verify and audit the refactored models to confirm the accuracy of their outputs?

Be prepared to justify your decisions and explain the reasoning behind the changes and discuss the pros and cons of your chosen method.

```sql
WITH filtered_orders AS (
   SELECT *
   FROM orders
   WHERE
       customer_id IN (
           SELECT customer_id
           FROM customers
           WHERE region = 'US'
           or region = 'Canada'
           or region = 'Mexico'
       )
       AND status = 'completed'

),

order_totals AS (
   SELECT
       o.id,
       o.customer_id,
       COUNT(item_id) as items_in_order,
       SUM(od.quantity * od.unit_price) OVER (PARTITION BY o.id) AS order_total
   FROM filtered_orders o
   LEFT JOIN order_items od ON o.id = od.order_id
  
)

SELECT DISTINCT
   fo.customer_id,
   cust.customer_name,
   COALESCE(fo.total_revenue_per_customer, 0) AS total_revenue
FROM customers AS cust
LEFT JOIN (
   select
       distinct customer_id as customer_id,
       SUM(order_total) AS total_revenue_per_customer
   FROM order_totals
   GROUP BY customer_id
      
   UNION
  
   SELECT 
       cust.id as customer_id,
       0 as total_revenue_per_customer
   FROM customers cust
   LEFT JOIN orders o 
      ON cust.id = o.customer_id
   WHERE o.id IS null

) fo
ON cust.id = fo.customer_id
ORDER BY total_revenue DESC;
```

## Explanation:

The query should be refactored to align with our existing dbt project structure. This will improve readability, code maintainability, and efficiency in using our models.

Instead of a single query, we will create the following models across different project layers:

1. **Staging** - `stg_customers`, `stg_orders`, `stg_order_items`
2. **Intermediate** - `dim_customers`, `fct_orders`
3. **Marts** - `mart_customer_revenue`

Staging layer has models with 1-1 relationship of raw data with minor transformations like renaming columns (`id → order_id`) or adjusting the data types.

**Staging models:**

```sql
-- models/stg/stg_customers

SELECT
		customer_id,
		customer_name,
		region AS customer_region
		-- Add additional fields from raw customers data
FROM {{ source('raw', 'customers') }}
```

```sql
-- models/stg/stg_orders

SELECT
		id AS order_id,
		customer_id,
		status AS order_status
		-- Add additional fields from raw orders data
FROM {{ source('raw', 'orders') }}
```

```sql
-- models/stg/stg_order_items

SELECT
		item_id,
		order_id,
		quantity,
		unit_price
		-- Add additional fields from raw order_items data
FROM {{ source('raw', 'order_items') }}
```

Let’s assume that our company uses Kimball (dimensional) data modelling approach, that’s why on intermediate layer we will create dimensional and fact models.

`dim_customers` - contains descriptive information about the customers

Depending on the data `order_items` could also be a dimensional model but for simplicity we will add it to the fact model.

**Dimension model:**

```sql
-- models/intermediate/dim_customers

SELECT *
FROM {{ ref('stg_customers') }}
```

Next, let's create our fact table.

`fct_orders` - contains business events, in our case customer orders.

We will add data from the `order_items` table, including:

- The number of items per order
- The total revenue per order

Instead of window function we will use aggregation to calculate `order_revenue`.

`SUM(od.quantity * od.unit_price) OVER (PARTITION BY o.id)` → 

→ `SUM(ot.quantity * ot.unit_price)` with GROUP BY 

It will improve readability and query performance as most query engines work slower with window functions.

**Fact model:**

```sql
-- models/intermediate/fct_orders

WITH orders AS (

SELECT *
FROM {{ ref('stg_orders') }}

),

order_items AS (

SELECT *
FROM {{ ref('stg_order_items') }}

)

SELECT
       o.order_id,
       o.customer_id,
       COUNT(ot.item_id) as items_in_order,
       SUM(ot.quantity * ot.unit_price) AS order_revenue
       -- Add additional fields to the table if needed
FROM orders AS o
LEFT JOIN order_items AS ot
		ON o.order_id = ot.order_id
GROUP BY o.order_id, o.customer_id
```

Let’s create the final model by combining fact and dimension tables.

We will implement the following improvements to our code:

- Instead of multiple `OR` filtering, we’ll use `IN` to filter `customer_region`. In improves readability.
- Using `LEFT JOIN` instead of `UNION` to get revenue per customer will improve query performance.
- Using models instead of subqueries - improves readibility and reusibility as we can build models based on the existing ones.

**Final mart model:**

```sql
-- models/marts/mart_customer_revenue

WITH filtered_dim_customers AS (

SELECT *
FROM {{ ref('dim_customers') }}
WHERE customer_region IN ('US','Canada','Mexico')

),

filtered_fct_orders AS (

SELECT *
FROM {{ ref('fct_orders') }}
WHERE order_status = 'completed'

)

SELECT 
		c.customer_id,
		c.customer_name,
		COALESCE(SUM(o.order_revenue),0) AS total_revenue
FROM filtered_dim_customers as c
LEFT JOIN filtered_fct_orders as o
		ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name
```

### Output Accuracy & Verification

1. **Data Comparison**

We can compare the data from the old query vs new query by using `EXCEPT` command:

```sql
SELECT * FROM old_query
EXCEPT
SELECT * FROM new_query
```

We can also compare the total row count or aggregations, such as revenue by month for each query.

Another option is to check edge cases, like customers without orders.

2. **Data Contracts**

We can use Data Contracts to verify the outputs of our models. They check if our model output contains the column names, data types, and constraints that we defined in the yml file.

Views support only `column names` and `data types`.

Example:

```yaml
models:
  - name: dim_customers
    config:
      contract:
        enforced: true
    columns:
      - name: customer_id
        data_type: int
        constraints:
          - type: primary_key
      - name: customer_name
        data_type: string
      - name: customer_region
	      data_type: string

```

3. **Tests**

We can ensure the data quality of our models by utilizing different types of tests. They can check if data in our fields is unique, doesn’t have null values, or perform more advanced logic checks.

For more advanced testing, we can write our own custom tests, use unit tests to validate transformation logic, or leverage packages like `dbt_expectations` for additional validation.

Example of built-in tests:

```yaml
models:
  - name: stg_customers
    columns:
      - name: customer_id
        data_tests:
          - unique
          - not_null
      - name: customer_name
      - name: customer_region
        data_tests:
          - accepted_values:
              values: ['US', 'Mexico','Canada']
```

1. **Monitoring & Alerts**

We can validate the outputs with stakeholders to ensure they align with their expectations.

In the BI tool, we can set up alerts for key metrics and receive notifications in Slack when an alert is triggered.


# Task 3

## Task 3: Peer review of a dbt model

Review the dbt model located at **models/stg/stg_shipment_customers.sql in repo data-take-home-assignment/** and share your feedback, including any suggestions or improvements you would make as part of a peer code review. Provide specific details about the aspects you’d give feedback on and explain why. Be ready to discuss your feedback.

```sql
-- models/stg/stg_customer_shipment_spend

WITH customers AS (
   SELECT
       id AS customer_id,
       city,
       province_code,
       email_address,
       phone_number,
       created_at AS registration_date
   FROM raw.database.shipments
   -- for simplicity we select all customers who send shipments
),

shipments AS (
   SELECT
       s.id AS shipment_id,
       s.customer_id,
       s.shipment_date,
       s.shipment_status,
       s.delivery_cost
   FROM {{ source('raw', 'shipments') }} s
   WHERE s.shipment_status IN ('completed', 'in_progress')
),

shipment_totals AS (
   SELECT
       shipments.customer_id,
       COUNT(shipments.shipment_id) AS total_shipment,
       SUM(shipments.delivery_cost) AS total_cost,
       MIN(shipments.shipment_date) AS first_shipment_date,
       MAX(shipments.shipment_date) AS last_shipment_date
   FROM shipments
   GROUP BY shipments.customer_id
),

customer_data AS (
   SELECT
       customers.customer_id,
       CONCAT(customers.city, ' ', customers.province_code) AS customer_location,
       customers.email_address,
       customers.phone_number,
       shipment_totals.total_shipment,
       shipment_totals.total_cost,
       shipment_totals.first_shipment_date,
       shipment_totals.last_shipment_date
   FROM customers
   LEFT JOIN shipment_totals ON customers.customer_id = shipment_totals.customer_id
)

SELECT
   customer_data.customer_id,
   customer_data.customer_location,
   customer_data.email_address,
   customer_data.phone_number,
   customer_data.total_shipment,
   customer_data.total_cost,
   CASE
       WHEN customer_data.total_cost > 1000 THEN 'High Value'
       WHEN customer_data.total_cost BETWEEN 500 AND 1000 THEN 'Medium Value'
       ELSE 'Low Value'
   END AS customer_type
FROM customer_data
ORDER BY customer_data.total_cost DESC;
```

## Feedback:

### 1. DBT Project Structure

We should use our company’s dbt project structure when creating models.

At our company, we use Kimball (dimensional) data modeling, which consists of three layers:

- **Staging**:  `stg_customers`, `stg_shipments` - 1-1 to raw data with minor transformations like renaming fields or changing data types.
- **Intermediate**: `dim_customers,` (`fct_shipments`) - dimensional and fact models
- **Marts**:  `mart_customer_shipment_spend` - final models

Every model should be built one by one in the following order: staging → intermediate → marts.

### 2. Source reference

- Instead of `raw.database.shipments`, we should use `{{ source('raw', 'shipments') }}`. Using `source()` and `ref()` in our dbt models allows us to track dependencies and follow model dependencies.
- Please, check if customer data should come from the shipments table. I assume there could be a dedicated customers table that we use instead (`{{ source('raw', 'customers') }}`)

### **3. Handling Null Values**

- If a customer has no shipments, `total_cost` and `total_shipment` could be `NULL`.
- To avoid nulls, it’s better to use `COALESCE`:

```sql
-- example
COALESCE(shipment_totals.total_cost, 0)
COALESCE(shipment_totals.total_shipment, 0)
```

### 4. Naming Conventions and Data Types

- It’s better to use clear and consistent names:
    - Rename `city` in the customers table to `customer_city`.
- Use aliases for tables in CTEs and be consistent across queries.
- Check data types:
    - `shipment_date` might be DATE or TIMESTAMP.
    - `delivery_cost` could be NUMERIC or INTEGER
- Document or enforce expected data types in staging models by using Data Contracts in associated yml file.

### 5. Documentation & Tests

- Add documentation for each dbt model with the table and fields description.
- Define schema tests (`unique`, `not_null`) on key fields like `customer_id` and `shipment_id`.
- Tests help to maintain good data quality and catch data issues.

### 6. Commenting & Readability

- Keep CTEs clear and add short comments where needed, especially when using complex SQL logic.
- Well-documented queries help others to better understand and maintain the models.
