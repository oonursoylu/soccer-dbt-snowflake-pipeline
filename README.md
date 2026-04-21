# European Soccer Analytics - Modern Data Stack (MDS) Pipeline

[![dbt](https://img.shields.io/badge/dbt-1.11+-FF694B?style=for-the-badge&logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Data_Cloud-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)](https://www.snowflake.com/)
[![AWS](https://img.shields.io/badge/AWS-S3_Stage-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white)](https://aws.amazon.com/)
[![dbt CI/CD Pipeline](https://img.shields.io/github/actions/workflow/status/oonursoylu/soccer-dbt-snowflake-pipeline/dbt_pipeline.yml?style=for-the-badge&logo=github&label=CI/CD)](https://github.com/oonursoylu/soccer-dbt-snowflake-pipeline/actions)
[![Tests](https://img.shields.io/badge/dbt_tests-101_passing-brightgreen?style=for-the-badge)](https://github.com/oonursoylu/soccer-dbt-snowflake-pipeline)

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
- **Infrastructure:** 7 staging views, 5 intermediate views, 3 SCD Type 2 snapshots, 4 materialized marts, and 2 Jinja macros.
- **Quality Assurance:** 101 dbt tests wired into a GitHub Actions CI pipeline. Every PR runs the full build against Snowflake before merge.

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
* **Source Data Quality Surfaced by Tests:** A range test on `age_at_rating` failed on ~16,000 rows during development — every one of them stamped with the same rating date (`2007-02-22`) and showing players aged 8-9. The shared timestamp gave it away as a source-system default for missing dates, not a pipeline bug. A documented filter in `int_player_age_analysis` (`rating_date >= '2008-01-01'`) removes these records before downstream aggregation. The fix is in the intermediate layer on purpose: staging preserves raw data untouched, and the business rule ("ignore pre-2008 default entries") belongs next to the models that depend on it.

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
Three snapshots track how player ratings, player physical profiles, and team tactical attributes change over time. All three use the `check` strategy: the source data has no reliable `updated_at` timestamp, and an earlier attempt using `timestamp` + `player_id` as the unique key surfaced duplicate-row errors because the source occasionally records multiple entries for the same player on the same day. Tracking specific columns (`overall_rating`, `potential` for players; `build_up_play_speed`, `defence_pressure`, `chance_creation_shooting` for teams) produces a clean SCD Type 2 history without depending on potentially dirty timestamps.

### 3. Reusable Business Logic via Jinja Macros
Two macros isolate logic that otherwise would have drifted across models:

- `is_favorite_upset` classifies each team's result against the bookmakers' implied prediction. It distinguishes three kinds of surprise: full upsets (favorite loses or underdog wins), draw upsets (a draw against a clear favorite — weighted as a half-surprise in the downstream index), and high-risk matches where the bookmaker had no clear favorite at all.
- `classify_tactical_score` bands raw FIFA tactical scores into High / Medium / Low tiers and propagates NULL instead of collapsing missing data into "Low" — the difference matters when aggregating team identity.

Changing a threshold or a category label is a one-file edit, not a search-and-replace across four marts.

### 4. Window Functions Split Across CTEs
Finding the exact date of a player's career peak requires applying one aggregate over the result of another — `MAX(rating_date WHERE rating = MAX(rating) OVER ...) OVER ...` — which Snowflake rejects. Breaking the calculation across two CTEs (compute the peak value in one, use it as a plain column in the next) sidesteps the restriction without losing clarity.

League standings use `RANK() OVER (PARTITION BY season, league_id ORDER BY total_points DESC, goal_difference DESC, total_goals_scored DESC)` — `RANK` rather than `DENSE_RANK` or `ROW_NUMBER` because real football tables share positions on ties and skip the next slot (e.g., 1, 2, 2, 4).

### 5. Data Quality Testing
The project runs 101 tests on every build. Most are generic (`not_null`, `unique`, `accepted_values`, `relationships`, `dbt_utils.accepted_range`, `dbt_utils.expression_is_true`) declared in YAML alongside the models they cover. Three are custom singular tests that generic tests cannot express:

- `assert_realistic_match_counts`: no team plays more than 50 league matches per season — catches unpivot fan-out bugs.
- `assert_valid_fifa_ratings`: initial and peak ratings stay within the real FIFA 1-99 range.
- `assert_wins_equal_losses_per_league_season`: within each league season, total wins across all teams must equal total losses (draws contribute to neither side). This is a cross-row invariant that generic column tests can't check — if it ever fails, there is either a duplicated match row or a broken unpivot upstream.

Mart-level invariants like `wins + draws + losses = matches_played`, `(wins * 3) + draws = total_points`, and `total_unpredictable_events = favorite_fails + underdog_wins + draw_upsets` are also enforced as `expression_is_true` tests — the kind of sanity check that turns an aggregation bug from a silent error into a loud one.

### 6. CI Pipeline
A GitHub Actions workflow runs `dbt build` against a dedicated Snowflake CI schema on every push to `main` and every pull request targeting it. If any of the 101 tests fail, the PR blocks merge. On successful main-branch runs, the workflow also regenerates and publishes the dbt docs site.

---

## Quick Start & Setup

**1. Clone the repository:**

```bash
git clone https://github.com/oonursoylu/soccer-dbt-snowflake-pipeline.git
cd soccer-dbt-snowflake-pipeline
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

This project includes full column-level documentation and an interactive DAG (Directed Acyclic Graph). To generate and serve it locally, run:

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
