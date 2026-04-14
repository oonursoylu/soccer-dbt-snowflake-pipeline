# European Soccer Analytics - Modern Data Stack (MDS) Pipeline

[![dbt](https://img.shields.io/badge/dbt-1.11+-FF694B?style=for-the-badge&logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Data_Cloud-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)](https://www.snowflake.com/)
[![AWS](https://img.shields.io/badge/AWS-S3_Stage-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white)](https://aws.amazon.com/)
[![dbt CI/CD Pipeline](https://github.com/oonursoylu/soccer-dbt-snowflake-pipeline/actions/workflows/dbt_pipeline.yml/badge.svg)](https://github.com/oonursoylu/soccer-dbt-snowflake-pipeline/actions)

**[View Interactive Data Dictionary & Lineage Graph](https://oonursoylu.github.io/soccer-dbt-snowflake-pipeline/)**

---

## Project Overview & Audience

This project is an end-to-end Analytics Engineering pipeline built to transform raw European soccer data into business-ready, high-performance analytical models.

**Target Audience:**
- **BI Analysts:** Query ready-to-use analytical tables directly from the `analytics_marts` schema in Snowflake.
- **Data Scientists & Engineers:** Leverage SCD Type 2 historical snapshots for exact point-in-time feature engineering. Ephemeral models are used for logical CTE abstraction and modularization, keeping the DAG clean without creating additional tables in Snowflake.

---

## Dataset & Project Scale

The pipeline processes the renowned Kaggle dataset: [European Soccer Database](https://www.kaggle.com/datasets/hugomathien/soccer), ingested into Snowflake via AWS S3.

- **Scale:** Over 25,000 matches, over 10,000 players, 11 major European Leagues
- **Timeline:** Covers 8 consecutive seasons of historical data
- **Infrastructure:** 7 Staging Views, 2 SCD Type 2 Snapshots, 5 Ephemeral Intermediate Models, and 4 Materialized Data Marts
- **Quality Assurance:** 60+ automated dbt data tests integrated into a continuous CI/CD pipeline (GitHub Actions), ensuring zero-breakage deployments.

---

## Architecture & Data Lineage

The project strictly follows a multi-layered, modular architecture based on Modern Data Stack principles, highlighting point-in-time historical tracking via Snapshots.

![dbt Data Lineage Graph](assets/dbt_lineage_graph.png)

---

## Key Business Insights Delivered

By querying the finalized Data Marts (`mart_`), the pipeline uncovered several compelling, data-driven historical insights:

* **The Elite Tiers (SCD Type 2 Depth):** The model identified all players who reached the legendary **90+ overall rating** milestone within the dataset. By preserving historical attribute changes via SCD Type 2 row insertions, the pipeline tracks the exact volume of updates (`total_updates`) and enables precise analytical calculations based on that history—such as identifying that **Lionel Messi** hit his absolute peak (94) at age 28, and **Cristiano Ronaldo** (93) at age 30.
* **Data-Driven Peak Age:** Statistical aggregation of historical player lifecycles reveals that a European soccer player hits their statistical prime (Peak FIFA Rating) at an average age of **26.1 years**, reflecting the underlying algorithms of the video game's attribute data.
* **The "Giant Killer" Index:** By algorithmically comparing bookmaker odds against actual match outcomes, the unpredictability model mathematically proved that the **Scottish Premiership** is the most volatile and unpredictable league, with favorites failing to win at a significantly higher rate (**Average Upset Rate: 36.8%**) than other top-tier European leagues.
* **Offensive Dominance:** Analyzing historical league standings shows that **Real Madrid CF (2011/2012 Season)** stands as the most lethal attacking side within the dataset's history (2008–2016), averaging a staggering **3.18 goals per game**.
* **The Invincibles (Peak Win Rate):** The pipeline identified the **2010/2011 FC Porto** squad as the most dominant single-season team, achieving a monumental **90.0% Win Rate** across their entire domestic campaign.

---

## Sample Query Output

> **Evidence of Production-Ready Models:** These are real results queried directly from the finalized `mart_` models in Snowflake, confirming the accuracy of the pipeline's logic.

### Team Performance Analytics (Offensive Dominance)
Verification of the 3.18 goals-per-game record:

![Mart Standings Output](assets/mart_standings_sample.png)

### Player Evolution (SCD Type 2 History)
Verification of player peak tracking and longitudinal data. The query securely filters for the 90+ elite club, calculating their exact age at their absolute peak and counting their total historical attribute updates to demonstrate data density:

![Mart Player Lifecycle Output](assets/mart_player_lifecycle_sample.png)

---

## Key Engineering Highlights

### 1. Cloud Data Extraction & AWS S3 Ingestion
Extracted raw relational data from a local `.sqlite` database, transformed it into structured flat files, and orchestrated the secure upload to an **AWS S3 Bucket**. Configured an S3 External Stage within Snowflake, utilizing optimized `COPY INTO` bulk loading to ingest **over 220,000 total historical records across all entities** into the `RAW` database layer before triggering the dbt pipeline.

### 2. Slowly Changing Dimensions (SCD Type 2)
Implemented dbt `snapshots` using a hybrid approach combining `timestamp` and `check` strategies to track historical changes in player physical attributes and team tactical metrics — enabling true point-in-time analysis without data loss.

### 3. DRY Principles with Modular Jinja Macros
Abstracted complex, repetitive business logic (betting upset identification, tactical threshold categorizations) into reusable **Jinja Macros** (`is_favorite_upset`, `classify_tactical_score`). Ensures a Single Source of Truth — if business definitions change, logic is updated in one place and propagates automatically.

### 4. Advanced Window Functions & CTE Stacking
- Solved SQL nested window function limitations (e.g., pinpointing the exact date of a career peak) via logical CTE stacking
- Utilized `RANK() OVER (PARTITION BY ...)` for dynamic league standings based on strict European tie-breaking rules (Points → Goal Difference → Goals Scored)

### 5. Rigorous Data Quality & Governance
- **60+ dbt tests** covering `not_null`, `unique`, and `accepted_values`.
- **4 Custom Singular Tests:** Implemented advanced SQL-based validations:
  - `assert_peak_rating_logic`: Validates that a player's peak rating mathematically cannot be lower than their initial rating.
  - `assert_realistic_match_counts`: Prevents data volume anomalies (fan-out) by capping maximum league matches.
  - `assert_valid_fifa_ratings`: Ensures attribute scores strictly fall within the official FIFA range (1-99).
  - `assert_valid_unpredictability_index`: Mathematically validates that the unpredictability percentage strictly falls within the valid 0-100 range.

### 6. Automated CI/CD & Documentation Deployment
Implemented a robust CI/CD workflow using **GitHub Actions**. Every push or pull request triggers an automated `dbt build` cycle in Snowflake, verifying model integrity and data quality tests before merging. Upon a successful build, the project's documentation and lineage DAG are automatically updated and hosted via **GitHub Pages**, providing a transparent and up-to-date data contract for all stakeholders.

---

## Quick Start & Setup

**1. Clone the repository:**

```bash
git clone https://github.com/oonursoylu/soccer-dbt-pipeline.git
cd soccer-dbt-pipeline
```

**2. Configure your `profiles.yml`:**

Add the following to `~/.dbt/profiles.yml`, replacing placeholders with your Snowflake credentials:

```yaml
soccer_analytics:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <your_account_id>
      user: <your_username>
      password: <your_password>
      role: SYSADMIN
      database: SOCCER_DB
      warehouse: COMPUTE_WH
      schema: analytics_marts
      threads: 4
```

**3. Install dependencies and run the pipeline:**

```bash
dbt deps      # Install dbt_utils
dbt seed      # Load static country/league mappings
dbt snapshot  # Capture initial historical states
dbt build     # Execute the entire DAG in order (runs models, tests, and snapshots)
```

**4. Explore the Data Dictionary:**

This project includes full column-level documentation and an interactive DAG. You can view the live version here: [Project Documentation](https://oonursoylu.github.io/soccer-dbt-snowflake-pipeline/)

Alternatively, to serve it locally:
```bash
dbt docs generate
dbt docs serve
```

The documentation will be available at `http://localhost:8080`.

---

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## Contact

**Onur Soylu** — Data / Analytics Engineer  
[LinkedIn Profile](https://www.linkedin.com/in/onur-soylu-0ba931119/) | [oonursoylu@gmail.com](mailto:oonursoylu@gmail.com)

*Feel free to reach out for feedback, questions, or collaboration!*
