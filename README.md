# Nexvia Retail & Supply Chain Control Tower

An end-to-end retail and supply chain analytics project designed to monitor inventory performance, track operational KPIs, detect anomalies, and forecast product demand using data engineering, analytics, and machine learning.

**Tech Stack:** PostgreSQL · Python · Pandas · Prophet · Apache Airflow · Docker · SQL

---

## 1. Project Overview

Modern retail supply chains face challenges such as unpredictable customer demand, inventory shortages, supplier delays, and limited operational visibility.

The **Nexvia Retail & Supply Chain Control Tower** aims to address these challenges by bringing historical supply chain data and simulated operational data into a unified analytics workflow.

The project combines database engineering, KPI development, automated pipeline execution, anomaly detection, and demand forecasting to provide insights into supply chain performance.

The system is designed to answer questions such as:

- Which products experience frequent demand?
- How efficiently is inventory being utilized?
- Where are potential stockout risks occurring?
- Are suppliers delivering within expected timelines?
- Are there unusual demand spikes?
- What could future product demand look like?
- How can automated monitoring improve operational visibility?

### Project Objectives

1. Build a structured PostgreSQL database for retail and supply chain analytics.
2. Develop SQL-based operational KPIs and analytical views.
3. Simulate operational inventory and supplier activity.
4. Automate recurring data checks and KPI monitoring using Apache Airflow.
5. Detect unusual demand patterns and supplier delays.
6. Develop aggregate and product-level demand forecasting models.
7. Evaluate forecasting methods using historical holdout data.
8. Establish a foundation for a real-time supply chain control tower dashboard.

---

## 2. System Architecture

The project follows a layered data and analytics architecture.

```text
                 DataCo Smart Supply Chain Dataset
                              |
                              v
                    PostgreSQL Database
                              |
                 +------------+------------+
                 |                         |
                 v                         v
          Historical Data             Simulated Data
             (dataco)                     (ops)
                 |                         |
                 +------------+------------+
                              |
                              v
                    SQL Analytics Layer
                              |
                   KPI Views and Metrics
                              |
                              v
                     Apache Airflow
                      Hourly Pipeline
                              |
              +---------------+---------------+
              |               |               |
              v               v               v
         Data Checks     KPI Refresh     Anomaly Detection
              |               |               |
              +---------------+---------------+
                              |
                              v
                       Python Analytics
                              |
                +-------------+-------------+
                |                           |
                v                           v
         Aggregate Forecasting      Product Forecasting
             (Prophet)             (Croston-SBA, Baselines)
                |                           |
                +-------------+-------------+
                              |
                              v
                    Analytics PostgreSQL
                              |
                              v
                 Future Control Tower Dashboard
```

### Main Components

| Component | Purpose |
|---|---|
| PostgreSQL | Data storage, transformation, and analytical queries |
| DataCo Dataset | Historical retail and supply chain information |
| Simulated Operations | Inventory, supplier, and hourly operational scenarios |
| SQL Views | KPI calculation and analytical data preparation |
| Python | Data processing, anomaly detection, and forecasting |
| Prophet | Aggregate demand forecasting |
| Croston-SBA | Experimental intermittent-demand forecasting |
| Apache Airflow | Scheduled pipeline orchestration |
| Docker | Airflow environment deployment |

---

## 3. Data Sources and Data Strategy

The project uses the DataCo Smart Supply Chain dataset as its historical foundation and separately maintains simulated operational data for testing operational workflows.

### 3.1 Historical Data — DataCo Smart Supply Chain

The DataCo dataset contains historical retail and supply chain information, including:

- Customer orders
- Order items
- Product information
- Product categories
- Order dates and statuses
- Product quantities
- Revenue-related information

The historical data is stored in the PostgreSQL schema:

```sql
dataco
```

The historical order data is used for:

- Sales and demand analysis
- Product-level demand distribution
- Historical KPI calculations
- Aggregate demand forecasting
- Product-level forecasting experiments

The historical dataset spans:

**January 2015 – January 2018**

### 3.2 Simulated Operational Data

A separate schema named `ops` contains simulated operational records created to demonstrate supply chain monitoring and inventory workflows.

These include:

- Supplier information
- Product-supplier mapping
- Warehouse inventory
- Purchase orders
- Supplier shipments
- Inventory snapshots
- Daily inventory movements
- Hourly inventory movements

The simulated data is used for testing:

- Inventory monitoring
- Supplier delivery performance
- Stockout scenarios
- Inventory replenishment scenarios
- Hourly KPI monitoring
- Supplier delay anomaly detection

> **Important:** Historical DataCo records and simulated operational records are maintained separately. Simulated November 2026 operations are not treated as actual historical transactions.

---

## 4. Technology Stack

| Technology | Application |
|---|---|
| PostgreSQL | Relational database and SQL analytics |
| Python | Data processing and modelling |
| Pandas | Data manipulation |
| NumPy | Numerical calculations |
| Prophet | Time-series forecasting |
| SQLAlchemy | Python-to-PostgreSQL connectivity |
| psycopg2 | PostgreSQL database driver |
| Apache Airflow 3.3.2 | Workflow orchestration |
| Docker Desktop | Containerized Airflow environment |
| Matplotlib | Forecast visualization |
| Visual Studio Code | Development environment |

---

## 5. Database Design

The PostgreSQL database is organized into separate schemas to distinguish source data, operational simulations, and analytical outputs.

### 5.1 `dataco` — Historical Data

This schema contains the imported historical DataCo dataset.

Important tables used in analytics include:

| Table | Purpose |
|---|---|
| `dataco.order_items` | Historical product quantities and order-item information |
| `dataco.products` | Product names, categories, and prices |
| `dataco.customer_orders` | Customer order dates and statuses |

Example: Historical daily demand calculation.

```sql
SELECT
    co.order_at::DATE AS demand_date,
    oi.product_id,
    SUM(oi.quantity) AS demand_units
FROM dataco.order_items oi
JOIN dataco.customer_orders co
    ON oi.order_id = co.order_id
WHERE co.order_status NOT IN (
    'CANCELED',
    'SUSPECTED_FRAUD'
)
GROUP BY
    co.order_at::DATE,
    oi.product_id
ORDER BY
    demand_date,
    oi.product_id;
```

### 5.2 `ops` — Operational Simulation

The `ops` schema contains simulated operational data.

Important tables include:

| Table | Purpose |
|---|---|
| `ops.product_suppliers` | Product-to-supplier mapping |
| `ops.inventory_ledger_daily` | Daily inventory movement records |
| `ops.inventory_snapshots` | Inventory snapshots |
| `ops.live_inventory_hourly` | Simulated hourly inventory movement |
| `ops.inventory_ledger_reorder_scenario` | Inventory replenishment scenario comparison |

Analytical views developed include:

| View | Purpose |
|---|---|
| `ops.v_daily_demand` | Daily demand analysis |
| `ops.v_daily_stockout_kpi` | Daily stockout KPI |
| `ops.v_days_of_inventory_kpi` | Inventory coverage analysis |
| `ops.v_hourly_demand_profile` | Hourly demand analysis |
| `ops.v_inventory_scenario_comparison` | Inventory scenario comparison |
| `ops.v_live_hourly_kpis` | Hourly operational KPI monitoring |

### 5.3 `analytics` — Forecasting and Anomaly Outputs

The `analytics` schema stores forecasting and anomaly detection results.

Important objects include:

| Object | Purpose |
|---|---|
| `analytics.demand_forecasts` | Aggregate demand forecast results |
| `analytics.product_demand_forecasts` | Product-level forecast results |
| `analytics.demand_spike_anomalies` | Historical demand spike anomalies |
| `analytics.v_supplier_delay_anomalies` | Supplier delay anomaly analysis |
| `analytics.v_control_tower_anomalies` | Combined anomaly monitoring |
| `analytics.hourly_anomaly_snapshots` | Hourly anomaly monitoring snapshots |

---

## 6. Key Performance Indicators (KPIs)

SQL views were developed to calculate important retail and supply chain performance indicators.

The current verified KPI results are:

| KPI | Result | Data Source |
|---|---:|---|
| Stockout Rate | 56.01% | Simulated Operations |
| Customer Shipping Punctuality | 42.69% | Historical DataCo |
| Supplier On-Time Performance | 57.50% | Simulated Operations |
| Average Supplier Lead Time | 6.61 days | Simulated Operations |
| Days of Inventory | 4.29 days | Simulated Operations |

These values represent the current project scenario and should not be interpreted as real-world company performance.

### KPI Interpretation

**Stockout Rate**

Measures the proportion of inventory demand that cannot be fulfilled because of insufficient available stock.

**Customer Shipping Punctuality**

Measures the percentage of historical customer orders meeting the project's shipping punctuality definition.

**Supplier On-Time Performance**

Measures the percentage of simulated supplier deliveries arriving within the defined expected timeline.

**Average Supplier Lead Time**

Measures the average time taken by simulated suppliers to deliver inventory.

**Days of Inventory**

Estimates how many days current inventory can support demand at the measured consumption rate.

---

## 7. Automated Pipeline — Apache Airflow

Apache Airflow is used to orchestrate recurring supply chain monitoring tasks.

The project uses Apache Airflow 3.3.2 with Docker.

### DAG: `hourly_supply_chain_pipeline`

The pipeline is configured with:

| Parameter | Configuration |
|---|---|
| DAG ID | `hourly_supply_chain_pipeline` |
| Schedule | `@hourly` |
| Catchup | `False` |
| Maximum Active Runs | `1` |
| Time Zone | `Asia/Kolkata` |
| Connection ID | `supply_chain_postgres` |

### Pipeline Responsibilities

The pipeline includes tasks for:

1. Checking source-data availability.
2. Validating important source-table record counts.
3. Refreshing operational KPI snapshots.
4. Monitoring demand and supplier anomalies.
5. Recording hourly anomaly monitoring results.

The source-data validation includes checks against:

```sql
SELECT COUNT(*) FROM dataco.order_items;
```

Expected record count:

```text
180519
```

And:

```sql
SELECT COUNT(*) FROM ops.inventory_ledger_daily;
```

Expected record count:

```text
32568
```

These checks help identify missing or unexpected source data before downstream tasks execute.

### Airflow Connection

The PostgreSQL connection is configured in Airflow using:

```text
Connection ID: supply_chain_postgres
```

Database credentials are configured through the Airflow connection rather than being hardcoded into the DAG.

---

## 8. Anomaly Detection

Anomaly detection is used to identify unusual demand behaviour and supplier delivery delays.

### 8.1 Demand Spike Detection

Demand spike detection identifies product demand observations that are unusually high compared with their historical baseline.

The project uses statistical anomaly detection based on a Z-score.

The formula is:

\[
Z = \frac{X-\mu}{\sigma}
\]

Where:

- \(X\) = Observed demand
- \(\mu\) = Historical baseline mean
- \(\sigma\) = Historical baseline standard deviation

The project applies validation conditions including:

- Z-score of at least 3
- Demand units of at least 3
- Positive baseline standard deviation

Detected anomalies are stored in:

```sql
analytics.demand_spike_anomalies
```

### 8.2 Supplier Delay Detection

Supplier delay monitoring evaluates simulated supplier delivery delays.

The current severity rules are:

| Delay | Severity |
|---|---|
| 2 days | WARNING |
| 3 or more days | CRITICAL |

The results are available through:

```sql
analytics.v_supplier_delay_anomalies
```

### 8.3 Combined Anomaly Monitoring

Demand spike and supplier delay anomalies are brought together through:

```sql
analytics.v_control_tower_anomalies
```

Hourly monitoring results are stored in:

```sql
analytics.hourly_anomaly_snapshots
```

This provides a foundation for tracking changes in supply chain risk over time.

---

## 9. Aggregate Demand Forecasting — Prophet

The first forecasting model estimates aggregate daily demand using historical DataCo order information.

### 9.1 Data Preparation

Historical demand is calculated by aggregating order-item quantities by date.

The forecasting experiment uses:

| Parameter | Value |
|---|---|
| Historical window | 2017-11-01 to 2018-01-31 |
| Historical observations | 92 days |
| Training period | First 62 days |
| Testing period | Final 30 days |
| Forecast horizon | 30 days |
| Model | Prophet |

Missing calendar dates are filled with zero demand.

Cancelled and suspected-fraud orders are excluded.

### 9.2 Model Configuration

The aggregate Prophet model uses:

- Flat growth
- Weekly seasonality enabled
- Yearly seasonality disabled
- Daily seasonality disabled
- Seasonality prior scale: 0.1
- Prediction interval width: 0.80

### 9.3 Model Evaluation

The reported holdout results are:

| Model | MAE |
|---|---:|
| Prophet | 1.603 |
| Naive Baseline | 1.529 |

Prophet did not outperform the naive baseline in this experiment.

This is retained as an important model evaluation result rather than assuming that a more complex model is automatically better.

### 9.4 Forecast Storage

Aggregate forecasts are saved to:

```sql
analytics.demand_forecasts
```

The latest reported run generated 30 forecast records.

The static forecast chart is saved as:

```text
dashboards/prophet_forecast_v3.png
```

---

## 10. Product-Level Demand Forecasting

Product-level forecasting is more challenging because many products have intermittent or sparse demand.

Instead of applying Prophet to every product, the project investigates methods appropriate for sparse demand patterns.

### 10.1 Historical Product Demand Analysis

The product-level analysis uses:

| Parameter | Value |
|---|---|
| Products in dataset | 118 |
| Historical window | 92 days |
| Start date | 2017-11-01 |
| End date | 2018-01-31 |

Demand distribution:

| Category | Products |
|---|---:|
| Products with frequent sales (15+ sales days) | 1 |
| Products with moderate sales (5–14 sales days) | 10 |
| Products with sparse sales (1–4 sales days) | 7 |
| Products with no sales in the period | 100 |
| **Total** | **118** |

This demonstrates that the product-level dataset is highly sparse during the selected analysis period.

### 10.2 Forecasting Methods Compared

Four approaches are evaluated:

**1. Croston-SBA**

Croston's method is designed for intermittent demand. It estimates non-zero demand size and the interval between non-zero demand events. The SBA adjustment is applied to reduce the bias associated with the original Croston method.

**2. Zero-Demand Baseline**

Predicts zero daily demand. This is a simple benchmark for products without observed demand.

**3. Naive Average**

Uses the average daily demand across the training period, including zero-demand days.

**4. Recent 14-Day Average**

Uses average daily demand over the most recent 14 training days.

### 10.3 Holdout Evaluation

The product-level backtest uses:

| Parameter | Value |
|---|---|
| Training period | 2017-11-01 to 2018-01-01 |
| Test period | 2018-01-02 to 2018-01-31 |
| Test horizon | 30 days |
| Products with training demand | 18 |
| Products with test demand | 9 |
| Products without training demand | 100 |

### 10.4 Preliminary Model Comparison

Reported overall results:

| Model | MAE | WAPE |
|---|---:|---:|
| Croston-SBA | 0.9103 | 163.49% |
| Zero baseline | **0.5568** | **100.00%** |
| Naive Average | 1.0286 | 184.75% |
| Recent 14-Day Average | 1.1089 | 199.16% |

Lower MAE and WAPE indicate better performance.

The zero-demand baseline achieved the lowest overall error in this holdout. However, its WAPE of 100% means it failed to predict any of the actual units sold.

Croston-SBA and the average-based approaches overpredicted demand in this experiment.

> **Evaluation caveat:** These are preliminary holdout results across all 118 products. One hundred products had no training demand, and only nine had any test-period demand. Therefore, the overall metrics should not be interpreted as evidence that zero-demand forecasting is operationally optimal. Active-product evaluation and additional rolling-origin backtesting are needed before selecting a production model.

### 10.5 Product Forecast Generation

The current product forecast generation logic uses:

- Croston-SBA for products with observed training demand.
- Zero baseline for products without observed training demand.

The latest reported run processed:

| Metric | Result |
|---|---:|
| Products processed | 118 |
| Products using Croston-SBA | 18 |
| Products using zero baseline | 100 |
| Forecast horizon | 30 days |
| Forecast records saved | 3,540 |

Forecasts are stored in:

```sql
analytics.product_demand_forecasts
```

The current forecast horizon is:

```text
2018-02-01 to 2018-03-02
```

These dates are historical forecast dates relative to the DataCo dataset, not live November 2026 demand predictions.

---

## 11. Project Directory Structure

The following is the intended high-level organization. Adjust individual filenames to match the final repository.

```text
supply-chain-control-tower/
│
├── airflow/
│   ├── dags/
│   │   └── hourly_supply_chain_pipeline.py
│   ├── docker-compose.yaml
│   └── .env.example
│
├── dashboards/
│   ├── prophet_forecast_v3.png
│   ├── product_backtest_metrics.csv
│   ├── product_backtest_daily_predictions.csv
│   ├── product_model_comparison.csv
│   └── active_product_model_comparison.csv
│
├── scripts/
│   ├── import_dataco.py
│   ├── detect_demand_spikes.py
│   ├── forecast_demand.py
│   ├── forecast_product_demand.py
│   └── backtest_prophet_v3.py
│
├── sql/
│   └── [SQL schema, table, and view scripts]
│
├── .gitignore
├── requirements.txt
└── README.md
```

Some generated CSV files may only exist after running the forecasting scripts.

---

## 12. Installation and Setup

### Prerequisites

Install the following:

- Python 3.10 or a compatible version for the selected dependencies
- PostgreSQL
- Visual Studio Code
- Docker Desktop
- Git

Apache Airflow should be installed using the project's Docker Compose configuration.

### Step 1: Clone the Repository

```bash
git clone <YOUR_GITHUB_REPOSITORY_URL>
cd supply-chain-control-tower
```

### Step 2: Create a Python Virtual Environment

Windows PowerShell:

```powershell
python -m venv .venv
```

Activate it:

```powershell
.venv\Scripts\Activate.ps1
```

### Step 3: Install Python Dependencies

If a `requirements.txt` file is available:

```powershell
pip install -r requirements.txt
```

The forecasting workflow uses packages including:

```text
pandas
numpy
prophet
sqlalchemy
psycopg2-binary
matplotlib
```

### Step 4: Configure PostgreSQL

Create the project database:

```sql
CREATE DATABASE supply_chain_db;
```

Configure your PostgreSQL connection using environment variables or a local configuration file that is excluded from Git.

Example configuration:

```text
DB_HOST=localhost
DB_PORT=5432
DB_NAME=supply_chain_db
DB_USER=your_username
DB_PASSWORD=your_password
```

Use the variable names actually expected by the project's scripts.

**Never commit passwords, database URLs containing credentials, or private API keys to GitHub.**

### Step 5: Import Historical Data

Place the DataCo dataset in the appropriate local data directory and run the project's import script.

```powershell
python scripts/import_dataco.py
```

Confirm that the historical tables were imported successfully before proceeding.

### Step 6: Configure Airflow

Move into the Airflow directory:

```powershell
cd airflow
```

Configure the Airflow environment using the provided `.env.example` or the project's configuration.

Start Airflow:

```powershell
docker compose up -d
```

Check the running services:

```powershell
docker compose ps
```

Configure the PostgreSQL connection in Airflow:

```text
Connection ID: supply_chain_postgres
```

Then open the Airflow web interface and enable the pipeline DAG.

### Step 7: Run Python Analytics

From the project root, run the relevant scripts:

```powershell
python scripts/detect_demand_spikes.py
```

Aggregate forecasting:

```powershell
python scripts/forecast_demand.py
```

Product-level forecasting and backtesting:

```powershell
python scripts/forecast_product_demand.py
```

Run the Prophet backtesting workflow:

```powershell
python scripts/backtest_prophet_v3.py
```

The exact execution order and database prerequisites should be followed as defined by each script.

---

## 13. Results and Generated Outputs

The project produces database records and local analytical outputs.

| Output | Description |
|---|---|
| `analytics.demand_forecasts` | Aggregate Prophet forecast results |
| `analytics.product_demand_forecasts` | Product-level forecast results |
| `analytics.demand_spike_anomalies` | Historical demand spike records |
| `analytics.hourly_anomaly_snapshots` | Hourly anomaly monitoring records |
| `dashboards/prophet_forecast_v3.png` | Aggregate forecast visualization |
| `dashboards/product_backtest_metrics.csv` | Product-level evaluation metrics |
| `dashboards/product_backtest_daily_predictions.csv` | Actual versus predicted daily demand |
| `dashboards/product_model_comparison.csv` | Overall four-model comparison |
| `dashboards/active_product_model_comparison.csv` | Active-product comparison, when generated |

### Forecast Validation

Example query for aggregate forecasts:

```sql
SELECT
    model_name,
    data_source,
    COUNT(*) AS forecast_records,
    MIN(forecast_date) AS first_forecast_date,
    MAX(forecast_date) AS last_forecast_date
FROM analytics.demand_forecasts
GROUP BY
    model_name,
    data_source;
```

Example query for product-level forecasts:

```sql
SELECT
    model_name,
    data_source,
    COUNT(*) AS forecast_records,
    COUNT(DISTINCT product_id) AS products_forecasted,
    MIN(forecast_date) AS first_forecast_date,
    MAX(forecast_date) AS last_forecast_date
FROM analytics.product_demand_forecasts
GROUP BY
    model_name,
    data_source;
```

---

## 14. Current Project Status

| Module | Status |
|---|---|
| DataCo dataset import | Completed |
| PostgreSQL normalization | Completed |
| Operational simulation tables | Completed |
| SQL KPI views | Completed |
| Airflow environment setup | Implemented |
| PostgreSQL Airflow connection test | Passed |
| Hourly pipeline development | Implemented |
| Demand spike detection | Implemented |
| Supplier delay anomaly detection | Implemented |
| Aggregate Prophet forecasting | Completed |
| Aggregate forecast evaluation | Completed |
| Product demand distribution analysis | Completed |
| Croston-SBA forecasting experiment | Completed |
| Four-model product backtesting | Completed |
| Active-product model comparison | Next evaluation step |
| Production forecasting model selection | In progress |
| Interactive control tower dashboard | Future development |

---

## 15. Limitations and Considerations

1. **Historical data coverage:** The DataCo dataset ends in January 2018. Forecast dates generated from this dataset should not be treated as current operational forecasts.

2. **Simulated operations:** Inventory and supplier operational results are simulated and are intended for workflow demonstration.

3. **Demand sparsity:** Only 18 of the 118 products had training demand in the selected 92-day period.

4. **Demand regime change:** Historical sales volume drops substantially from October 2017 compared with earlier months. This requires further investigation before extending the training window.

5. **Forecasting performance:** Prophet did not outperform the naive baseline in the reported aggregate holdout. Product-level Croston-SBA and average-based models also underperformed the zero baseline in the initial overall comparison.

6. **Model selection:** Additional rolling-origin evaluation and active-product analysis are required before selecting a final forecasting strategy.

7. **Operational deployment:** Simulated data and historical forecasts are not substitutes for integration with a live retail ERP, warehouse management system, or supplier data source.

---

## 16. Future Improvements

The next development stages include:

- Complete active-product forecasting evaluation.
- Investigate the historical sales-volume decline.
- Implement rolling-origin cross-validation.
- Evaluate alternative intermittent-demand methods.
- Improve supplier lead-time prediction.
- Develop inventory reorder recommendations.
- Add demand-versus-stock risk analysis.
- Integrate forecast outputs with simulated inventory.
- Build an interactive control tower dashboard.
- Add alerting for critical stockout and supplier-delay risks.
- Explore automated replenishment policy optimization.

---

## 17. Learning Outcomes

This project demonstrates practical experience in:

- Relational database design and normalization
- PostgreSQL and advanced SQL queries
- Data cleaning and transformation
- KPI development and analytical views
- Python-based data processing
- Time-series forecasting
- Intermittent-demand modelling
- Model evaluation using MAE and WAPE
- Statistical anomaly detection
- Workflow orchestration using Apache Airflow
- Docker-based environment setup
- Separation of historical and simulated data
- End-to-end analytics pipeline development

---

## 18. Acknowledgements

The historical dataset used in this project is the **DataCo Smart Supply Chain dataset**.

All simulated operational inventory and supplier scenarios are created for project development and demonstration purposes.

---

## 19. Author

**Anand Kumar Pandit**

GitHub: [Add your GitHub profile URL]

LinkedIn: [Add your LinkedIn profile URL]

---

*This is an evolving academic and portfolio project. Forecasting results, operational simulations, and system capabilities are documented according to the current development stage.*
