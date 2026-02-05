CREATE DATABASE IF NOT EXISTS casino_bi;
USE casino_bi;


-- Staging table (CSV import target)
CREATE TABLE IF NOT EXISTS stg_casino_revenue (
    rundate        DATETIME,
    source_file    VARCHAR(255),
    report_name    VARCHAR(255),
    year           INT,
    month          VARCHAR(20),
    row_name       VARCHAR(255),
    current_month  DECIMAL(18,2),
    calendar_ytd   DECIMAL(18,2),
    fiscal_ytd     DECIMAL(18,2),
    currency       VARCHAR(10)
);


-- Fact table with parsed fields and line classification
CREATE TABLE IF NOT EXISTS fact_casino_revenue AS
SELECT
    rundate,
    year,
    month,
    currency,
    report_name,

    TRIM(SUBSTRING_INDEX(report_name, '-', 1))  AS report_category,
    TRIM(SUBSTRING_INDEX(report_name, '-', -1)) AS casino_name,

    row_name,
    current_month,
    calendar_ytd,
    fiscal_ytd,

    CASE
        WHEN LOWER(TRIM(row_name)) = 'total'
            THEN 'TOTAL'

        WHEN LOWER(TRIM(row_name)) LIKE 'gross terminal revenue%'
            THEN 'GTR'

        WHEN LOWER(TRIM(row_name)) = 'non-banked games'
            THEN 'NON_BANKED'

        WHEN LOWER(TRIM(row_name)) = 'banked games'
            THEN 'BANKED'

        ELSE 'ALLOCATION'
    END AS line_type

FROM stg_casino_revenue;


-- Indexes for Tableau performance and reconciliation queries
CREATE INDEX idx_fact_group
    ON fact_casino_revenue (year, month, report_category, casino_name);

CREATE INDEX idx_fact_line_type
    ON fact_casino_revenue (line_type);



-- Reconciliation view implementing business rules
CREATE OR REPLACE VIEW vw_recon_rules AS
SELECT
    year,
    month,
    report_category,
    casino_name,

    MAX(CASE WHEN line_type = 'TOTAL' THEN current_month END) AS total_value,
    MAX(CASE WHEN line_type = 'GTR'   THEN current_month END) AS gtr_value,

    SUM(CASE
            WHEN line_type = 'BANKED'
            THEN COALESCE(current_month, 0)
            ELSE 0
        END) AS banked_value,

    SUM(CASE
            WHEN line_type = 'NON_BANKED'
            THEN COALESCE(current_month, 0)
            ELSE 0
        END) AS non_banked_value,

    SUM(CASE
            WHEN line_type = 'ALLOCATION'
            THEN COALESCE(current_month, 0)
            ELSE 0
        END) AS allocation_sum,

    CASE
        WHEN MAX(CASE
                    WHEN line_type IN ('BANKED','NON_BANKED')
                    THEN 1 ELSE 0
                 END) = 1
        THEN 1
        ELSE 0
    END AS has_banked_breakdown,

    (
        MAX(CASE WHEN line_type = 'TOTAL' THEN current_month END)
        - MAX(CASE WHEN line_type = 'GTR' THEN current_month END)
    ) AS diff_total_minus_gtr,

    (
        SUM(CASE
                WHEN line_type IN ('BANKED','NON_BANKED')
                THEN COALESCE(current_month, 0)
                ELSE 0
            END)
        - MAX(CASE WHEN line_type = 'GTR' THEN current_month END)
    ) AS diff_games_minus_gtr,

    (
        SUM(CASE
                WHEN line_type = 'ALLOCATION'
                THEN COALESCE(current_month, 0)
                ELSE 0
            END)
        - MAX(CASE WHEN line_type = 'TOTAL' THEN current_month END)
    ) AS diff_alloc_minus_total

FROM fact_casino_revenue
GROUP BY year, month, report_category, casino_name;



-- Tableau reporting views
CREATE OR REPLACE VIEW vw_tableau_revenue_main AS
SELECT
    year,
    month,
    report_category,
    casino_name,
    row_name,
    line_type,
    current_month,
    calendar_ytd,
    fiscal_ytd,
    currency
FROM fact_casino_revenue;


CREATE OR REPLACE VIEW vw_tableau_recon AS
SELECT
    year,
    month,
    report_category,
    casino_name,
    total_value,
    gtr_value,
    banked_value,
    non_banked_value,
    allocation_sum,
    has_banked_breakdown,
    diff_total_minus_gtr,
    diff_alloc_minus_total,
    CASE
        WHEN has_banked_breakdown = 1
        THEN diff_games_minus_gtr
        ELSE NULL
    END AS diff_games_minus_gtr_display
FROM vw_recon_rules;



-- Issues view (rows that fail reconciliation thresholds)
CREATE OR REPLACE VIEW vw_tableau_issues AS
SELECT *
FROM vw_recon_rules
WHERE ABS(diff_total_minus_gtr) > 5
   OR ABS(diff_alloc_minus_total) > 50
   OR (has_banked_breakdown = 1 AND ABS(diff_games_minus_gtr) > 5);



-- Quick validation queries
SELECT line_type, COUNT(*) AS row_count
FROM fact_casino_revenue
GROUP BY line_type
ORDER BY row_count DESC;


SELECT *
FROM vw_tableau_issues
ORDER BY ABS(diff_alloc_minus_total) DESC
LIMIT 20;
