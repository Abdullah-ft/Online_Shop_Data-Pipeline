# 🛒 Online Shop Data Pipeline (ETL + Churn Model)

AI Engineering Bootcamp · Data Warehousing and ETL · Solo project

---

## 📌 1. What I built

An online shop in Pakistan keeps orders in two places: a **PostgreSQL** database (website) and a **MongoDB** collection (mobile app). Management wanted three things. This pipeline delivers all three.

| #   | Management question                       | What I built                                               | ✅  |
| --- | ----------------------------------------- | ---------------------------------------------------------- | --- |
| 1   | How much did we sell, where, and of what? | One clean `orders` table, a star schema and 3 gold tables  | ✅  |
| 2   | Where did each customer live over time?   | `dim_customer` as **SCD Type 2** (history of city changes) | ✅  |
| 3   | Which customers stopped buying?           | A churn model, with every run tracked in **MLflow**        | ✅  |

### 🔄 Pipeline flow

```
PostgreSQL ─┐
MongoDB ────┼─► Extract (Colab) ─► Clean + Combine ─► Star schema ─► Delta tables ─► Gold tables (Spark) ─► Churn model (MLflow)
Weather API ┘      Part 1             Part 2            Part 3        Part 4            Part 5                 Part 6
```

### 🧰 Tools

Google Colab · PostgreSQL (Neon) · MongoDB (Atlas) · Open-Meteo API · pandas · Databricks Free Edition · Spark · Delta Lake · MLflow · scikit-learn

### 📁 Repository order

| Order | File                | Where it runs | Covers       |
| ----- | ------------------- | ------------- | ------------ |
| 1     | Colab notebook      | Google Colab  | Parts 1 to 3 |
| 2     | Databricks notebook | Databricks    | Parts 4 to 6 |

> 🔐 **No connection strings or passwords are in this repository.** They were read from Colab Secrets with `userdata.get(...)`.

---

## 📥 2. Extract (Part 1)

All raw data was saved to `raw/` without changes, with the extraction date in each file name.

| Source                    | Result                                                               |
| ------------------------- | -------------------------------------------------------------------- |
| 🐘 PostgreSQL `customers` | 2,000 rows                                                           |
| 🐘 PostgreSQL `orders`    | 200,000 rows, read in 4 chunks of 50,000                             |
| 🍃 MongoDB `shop.orders`  | 5,000 documents, saved as JSON before flattening                     |
| 🌦️ Weather API            | Lahore, Karachi, Islamabad (3 files, daily max temperature and rain) |

That makes **6 raw files**.

⏱️ **Incremental extract:** `extract_incremental(last_run)` returns only rows with `updated_at` after `last_run`.
With `last_run = "2026-10-01"` it returned **8,593 rows**.

---

## 🧹 3. Transform and clean (Part 2)

- The MongoDB documents were flattened with `json_normalize`.
- `customer.id` and `customer_id` were merged into one `customer_id` column.
- The nested `items` list gave `category` and `quantity`. `payment.method` became `payment_method`.
- Column names, types and date formats now match PostgreSQL.
- A `source` column was added: `web` or `app`.

### 🔁 Duplicate rule

> **One row per `order_id`. Keep the row with the latest `updated_at`. On a tie, keep the app row.**

- All **5,000** app order IDs also exist in PostgreSQL.
- Rows before: **205,000**. Rows after: **200,000**.
- Final mix: 195,000 `web` and 5,000 `app`.

### ✅ Data quality checks

| #   | Check                                           | Result  |
| --- | ----------------------------------------------- | ------- |
| 1   | No null `order_id`                              | ✅ PASS |
| 2   | `order_id` is unique                            | ✅ PASS |
| 3   | `amount` greater than 0                         | ✅ PASS |
| 4   | Every `customer_id` exists in `customers`       | ✅ PASS |
| 5   | No order dated in the future (after 2026-10-24) | ✅ PASS |
| 6   | `status` is one of the 5 known values           | ✅ PASS |

### 🌦️ Weather

The 3 weather responses became one table: `city`, `date`, `max_temp`, `rain_mm`. It is joined to orders on **city and order date**. Orders from other cities have no weather, which is expected.

---

## ⭐ 4. Warehouse design (Part 3)

```
dim_customer (customer_key PK, customer_id, name, city, segment, signup_date,
              valid_from, valid_to, is_current)
dim_date     (date_key PK, date, day, day_of_week, month, month_name, quarter, year, is_weekend)
dim_category (category_key PK, category)

fact_orders  (order_id PK, customer_key FK, date_key FK, category_key FK,
              customer_id, city, category, quantity, amount, status,
              payment_method, source, updated_at, order_date, max_temp, rain_mm)
```

I kept `city`, `category` and weather in `fact_orders` on purpose. It makes the Part 5 queries simpler.

### 📅 dim_date

One row per day from 2025-01-01 to 2026-12-31: **730 rows**. Includes day of week, month, quarter and `is_weekend`.

### 🕰️ dim_customer (SCD Type 2)

- The app orders hold the city **at order time**. PostgreSQL holds the **current** city.
- If they differ, the customer moved. A mover gets **two rows**: old city and new city, with `valid_from`, `valid_to` and `is_current`.
- Total rows: 2,000 + number of movers = **2000 rows** (**57 movers**).
- Each order joins to the row where `valid_from <= order_date <= valid_to`.

**Example mover: customer 5 (Hamza Qureshi)** — _check this against your final output and update the dates if they changed._

| customer_key | city    | valid_from | valid_to   | is_current |
| ------------ | ------- | ---------- | ---------- | ---------- |
| 5            | Quetta  | 2025-01-01 | 2026-07-19 | False      |
| 6            | Karachi | 2026-07-20 | 2099-12-31 | True       |

His orders from **February 2026** join to the **Quetta row**, because 2025-01-01 ≤ order date ≤ 2026-04-14.

> ⚠️ **Honest limit:** the move dates are **estimates**. Only 5,000 of 200,000 orders show the city at order time. I assume moves happened in 2026 and place each move in the **middle of the window** between the last old-city order and the first new-city order. About 0.7% of orders may sit on the wrong city row.

### 🔎 Checks on the final fact table

- `fact_orders` has **200,000 rows** and `order_id` is unique.
- Every order matches exactly one `dim_customer` row.
- Weather is filled on **115,457** orders (Lahore, Karachi and Islamabad only).

---

## 🧱 5. Delta tables in Databricks (Part 4)

Tables were saved in the schema `workspace.online_shop`:
`fact_orders`, `dim_customer`, `dim_date`, `dim_category`.

| Step                                                        | Result               |
| ----------------------------------------------------------- | -------------------- |
| Version 0 (first load)                                      | **197153 rows**      |
| Incremental load (`MERGE`, orders updated after 2026-10-20) | **2847 rows** merged |
| Current version                                             | **200,000 rows**     |

📸 Screenshot of `DESCRIBE HISTORY` is attached: version 0 and version 1 are the initial load the `197,153 rows`, version 2 is the merge `2847`.

---

## ⚡ 6. Gold tables with Spark (Part 5)

**Business rule:** revenue counts only orders that are **not** `cancelled` or `returned`.

| Table                    | Built with    | Content                                                                                 |
| ------------------------ | ------------- | --------------------------------------------------------------------------------------- |
| `gold_monthly_sales`     | DataFrame API | Revenue, order count, average order value by month and city                             |
| `gold_category_mix`      | DataFrame API | Revenue share of each category by month                                                 |
| `gold_customer_features` | Spark SQL     | Per customer: total spend, orders, days since last order, favourite category, app share |

### 💡 Answers (from queries on the gold tables)

**1️⃣ Which city grew fastest in 2026?**
**Lahore**, with **+7.1%** revenue (Jan to Sep 2026 vs Jan to Sep 2025), ahead of Multan (+6.3%) and Karachi (+5.2%). I used Jan to Sep because October 2026 ends on the 24th.

**2️⃣ Do Lahore, Karachi and Islamabad get fewer orders on rainy days?**
**No.** Orders per day are about the same on rainy and dry days, so there is no clear rain effect.

| City      | Dry days | Rainy days |
| --------- | -------- | ---------- |
| Lahore    | 64.6     | 65.1       |
| Karachi   | 75.7     | 77.2       |
| Islamabad | 33.7     | 34.0       |

(Rainy means more than 1 mm of rain.)

**3️⃣ Which category has the highest average order value?**
**Electronics**, at about **78,494** per order. Next are Home (35,105) and Fashion (13,437).

---

## 🤖 7. Churn model with MLflow (Part 6)

- **Cutoff:** 2026-08-01. Features use only orders **before** the cutoff, so there is **no leakage**.
- `days_since_last_order` is measured from the cutoff.
- Features: total spend, number of orders, average order value, days since last order, orders in the last 90 days, app share, number of categories.

### 🏷️ The label

| Label             | Definition                             | Churners                             |
| ----------------- | -------------------------------------- | ------------------------------------ |
| Brief's label     | No order from 2026-08-01 to 2026-10-24 | **2 of 2,000** (too few to train on) |
| My stricter label | No order from 2026-09-24 to 2026-10-24 | **148 of 2,000** (7.4%)              |

Customers in this dataset order very often, so the brief's label gave almost no churners. I kept it as evidence, and added the 30-day label as **my own extra experiment**. Features still use only pre-cutoff orders.

### 📊 Runs compared

| Model                         | Accuracy | Recall    | Precision | F1    | ROC AUC   |
| ----------------------------- | -------- | --------- | --------- | ----- | --------- |
| Logistic regression           | 0.574    | 0.784     | 0.124     | 0.214 | 0.764     |
| **Random forest, depth 3** ⭐ | 0.564    | **0.838** | 0.128     | 0.221 | **0.779** |
| Random forest, depth 8        | 0.838    | 0.432     | 0.211     | 0.283 | 0.757     |

Each run logs its parameters, the Delta version of the features table (version 0) and 5 metrics.

### 🏆 Chosen model: random forest, depth 3

I chose it because it had the highest recall (0.84) and the highest ROC AUC (0.78). Marketing cares most about recall: a missed churner is a lost customer, while a wasted win-back email costs little. Depth 8 has higher accuracy (0.84) but catches only 43% of churners, and accuracy is misleading because only about 7% of customers churn.

⚠️ Precision is low (about 0.13) for all models, so this is a **weak first version**.

🔁 The chosen model was reloaded by run ID and used to score 5 customers.
**Run ID:** `print(run_ids["rf_depth3"])`

---

## 📸 8. Screenshots included

- `DESCRIBE HISTORY` on `fact_orders`
- MLflow run comparison

---

## 🚧 9. Known limits

- SCD move dates are estimates (see section 4).
- The brief's churn label gave only 2 churners, so I added a stricter label.
- Precision of the churn model is low. Scores are modest.
- Weather exists only for Lahore, Karachi and Islamabad.
