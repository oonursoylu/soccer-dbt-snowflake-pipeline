# Analytics Engineering Pipeline with dbt, Snowflake, AWS, and CI/CD

[![dbt](https://img.shields.io/badge/dbt-1.11+-FF694B?style=for-the-badge&logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Data_Cloud-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)](https://www.snowflake.com/)
[![AWS](https://img.shields.io/badge/AWS-S3_Stage-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white)](https://aws.amazon.com/)
[![dbt CI/CD Pipeline](https://img.shields.io/github/actions/workflow/status/oonursoylu/soccer-dbt-snowflake-pipeline/dbt_pipeline.yml?style=for-the-badge&logo=github&label=CI/CD)](https://github.com/oonursoylu/soccer-dbt-snowflake-pipeline/actions)
[![Tests](https://img.shields.io/badge/dbt_tests-101_passing-brightgreen?style=for-the-badge)](https://github.com/oonursoylu/soccer-dbt-snowflake-pipeline)
[![dbt Docs](https://img.shields.io/badge/dbt_Docs-Live_Site-10B981?style=for-the-badge&logo=readthedocs&logoColor=white)](https://oonursoylu.github.io/soccer-dbt-snowflake-pipeline/)

## Overview

This project is an end-to-end analytics engineering pipeline built with **dbt, Snowflake, AWS S3, and GitHub Actions**.

It uses the public European Soccer Database as the source data. The main focus is not football analysis itself, but the analytics engineering workflow around it: loading raw data into Snowflake, transforming it with dbt, testing the models, documenting the pipeline, and creating clean marts for analysis.

The final output is a small analytics warehouse with tested marts for team performance, player development, league standings, and match predictability.

**Main project highlights:**

- Snowflake warehouse with raw and analytics layers
- dbt staging, intermediate, snapshot, and mart models
- 3 SCD Type 2 snapshots for historical tracking
- 4 materialized marts for analysis
- 101 dbt tests, including custom data quality tests
- GitHub Actions workflow running `dbt build`
- Published dbt Docs with lineage and model documentation

---

## Why I Built This Project

I built this project to practice the type of work an Analytics Engineer or Junior Data Engineer does in a modern data stack:

- turn raw relational data into clean analytical models
- design dbt layers that are easy to understand and maintain
- add tests so data issues are caught early
- document models for other data users
- use CI/CD to validate changes before they are merged

The dataset is football-related, but the project is focused on general data modeling, testing, documentation, and warehouse design skills.

---

## Tech Stack

| Area | Tool |
|---|---|
| Data warehouse | Snowflake |
| Transformation | dbt Core |
| Cloud storage | AWS S3 |
| CI/CD | GitHub Actions |
| Testing | dbt tests + dbt_utils |
| Documentation | dbt Docs |
| Source data | SQLite / Kaggle European Soccer Database |

---

## Dataset and Scale

The project uses the [European Soccer Database](https://www.kaggle.com/datasets/hugomathien/soccer) dataset.

| Metric | Value |
|---|---:|
| Matches | 25,000+ |
| Players | 10,000+ |
| Leagues | 11 |
| Seasons | 8 |
| Total ingested records | 220,000+ |
| Staging models | 7 |
| Intermediate models | 5 |
| Snapshots | 3 |
| Materialized marts | 4 |
| Jinja macros | 2 |
| dbt tests | 101 |

---

## Pipeline Architecture

```text
SQLite source data
        ↓
Extract to flat files
        ↓
AWS S3 bucket
        ↓
Snowflake RAW layer
        ↓
dbt staging models
        ↓
dbt intermediate models
        ↓
dbt snapshots
        ↓
dbt marts
        ↓
dbt docs + CI checks
```

![dbt Data Lineage Graph](assets/dbt_lineage_graph.png)

Live documentation:  
[dbt Docs](https://oonursoylu.github.io/soccer-dbt-snowflake-pipeline/)

---

## Final Data Marts

The project creates four materialized mart models.

| Mart | Purpose |
|---|---|
| `mart_team_season_standings` | Team-level season performance: points, wins, losses, goals, ranking |
| `mart_player_lifecycle` | Player rating history, peak rating, peak age, and number of updates |
| `mart_match_predictability` | Match outcome analysis based on bookmaker odds and actual results |
| `mart_team_tactical_profile` | Team tactical profile based on historical team attributes |

These marts are easier to query than the raw normalized source tables and are designed for downstream analysis.

---

## Example Questions Answered

The final marts can answer questions like:

- Which teams had the strongest single-season performance?
- At what age do players usually reach their highest rating?
- Which leagues had the most unexpected match results?
- How did player ratings change over time?
- Are there source data problems that need to be handled before analysis?

---

## Selected Results

### Player lifecycle analysis

Using SCD Type 2 snapshots, the project tracks player rating changes over time instead of relying only on the latest available record.

For example, the model can identify peak rating, peak age, and the number of historical updates for each player.

### Peak player age

Aggregating player lifecycle records shows that the average player in the dataset reaches peak rating at about **25.6 years old**.

### Match predictability

The match predictability mart compares bookmaker odds with actual match results and identifies leagues where favorites dropped points more often.

### Team performance

The team standings mart calculates season-level metrics such as points, win rate, goal difference, ranking, and goals per game.

---

## Data Quality Example

During development, a range test on `age_at_rating` failed for around 16,000 rows.

Those rows had the same rating date, `2007-02-22`, and unrealistic player ages of 8 or 9. This looked like a source-system default date, not a transformation bug.

I handled this in the intermediate layer with a documented filter:

```sql
rating_date >= '2008-01-01'
```

I kept the staging layer close to the raw source and applied the rule only where the player age analysis depends on valid rating dates.

---

## Key Engineering Details

### 1. AWS S3 and Snowflake loading

The raw SQLite data was exported to flat files, uploaded to AWS S3, and loaded into Snowflake using an external stage and `COPY INTO`.

This created a separate RAW layer before dbt transformations were executed.

### 2. SCD Type 2 snapshots

The project uses three dbt snapshots to track historical changes in:

- player ratings
- player physical attributes
- team tactical attributes

The snapshots use the `check` strategy because the source data does not have a reliable `updated_at` column.

### 3. Reusable Jinja macros

Two Jinja macros keep repeated logic consistent:

- `is_favorite_upset`: classifies match results against bookmaker expectations
- `classify_tactical_score`: groups tactical scores into High / Medium / Low categories while keeping `NULL` values unchanged

This keeps repeated logic in one place instead of duplicating it across multiple models.

### 4. SQL modeling

The project uses CTEs, joins, aggregations, window functions, and ranking logic.

For league standings, I used `RANK()` instead of `ROW_NUMBER()` because teams can share the same position when tied.

```sql
RANK() OVER (
    PARTITION BY season, league_id
    ORDER BY total_points DESC, goal_difference DESC, total_goals_scored DESC
)
```

### 5. Data quality testing

The project includes 101 dbt tests. These include:

- `not_null`
- `unique`
- `accepted_values`
- `relationships`
- `dbt_utils.accepted_range`
- `dbt_utils.expression_is_true`
- custom singular tests

Examples of custom tests:

- No team should play more than 50 league matches in one season
- FIFA-style ratings should stay between 1 and 99
- Total wins should equal total losses within each league season

These tests help catch issues such as duplicated rows, broken unpivot logic, invalid ratings, and inconsistent aggregate metrics.

### 6. CI/CD workflow

A GitHub Actions workflow runs `dbt build` against a Snowflake CI schema on pull requests and pushes to `main`.

If a model or test fails, the workflow fails.

On successful main-branch runs, the workflow also regenerates and publishes the dbt Docs site.

---

## Sample Outputs

### Team performance mart

![Mart Standings Output](assets/mart_standings_sample.png)

### Player lifecycle mart

![Mart Player Lifecycle Output](assets/mart_player_lifecycle_sample.png)

---

## Repository Structure

```text
.
├── .github/workflows/       # GitHub Actions workflow for dbt CI/CD
├── analyses/                # SQL queries used for validation and exploration
├── assets/                  # README images and sample query outputs
├── infrastructure/          # Snowflake, AWS, and ingestion setup files
├── macros/                  # Reusable dbt Jinja macros
├── models/                  # dbt models
│   ├── staging/             # Source-level cleaning and standardization
│   ├── intermediate/        # Business logic and reusable transformations
│   └── marts/               # Final analytics-ready mart models
├── seeds/                   # Static mapping tables
├── snapshots/               # dbt snapshots for historical tracking
├── tests/                   # Custom singular dbt tests
├── dbt_project.yml          # Main dbt project configuration
├── packages.yml             # dbt package dependencies
├── package-lock.yml         # Locked dbt package versions
├── .gitignore               # Ignored local/generated files
├── LICENSE
└── README.md
```

---

## How to Run the Project

### 1. Clone the repository

```bash
git clone https://github.com/oonursoylu/soccer-dbt-snowflake-pipeline.git
cd soccer-dbt-snowflake-pipeline
```

### 2. Configure dbt profile

Add this to `~/.dbt/profiles.yml` and replace the placeholders with your Snowflake credentials:

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

### 3. Install dependencies and run dbt

```bash
dbt deps
dbt seed
dbt snapshot
dbt build
```

### 4. Generate local docs

```bash
dbt docs generate
dbt docs serve
```

The docs will be available at:

```text
http://localhost:8080
```

---

## What This Project Shows

This project shows my ability to:

- build dbt models from raw source data
- design staging, intermediate, snapshot, and mart layers
- work with Snowflake and AWS S3
- write analytical SQL with window functions and aggregations
- add automated data quality tests
- use dbt snapshots for historical tracking
- document models and columns with dbt Docs
- use GitHub Actions for a basic CI workflow

---

## Possible Next Improvements

- Add Airflow to schedule the ingestion and dbt build steps
- Add incremental models for larger datasets
- Add a small BI dashboard on top of the marts
- Add more environment separation between development and production schemas

---

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## Contact

**Onur Soylu** — Data / Analytics Engineer  
[LinkedIn Profile](https://www.linkedin.com/in/onur-soylu-0ba931119/) | [oonursoylu@gmail.com](mailto:oonursoylu@gmail.com)

