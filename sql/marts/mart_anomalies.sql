-- Transparent, non-ML anomaly detection via rolling-median / robust
-- (MAD-based) z-score, computed entirely in SQL with window functions.
-- Rationale (docs/METRICS.md, "Anomaly detection"):
--   * median (not mean) as the rolling baseline resists being pulled by the
--     very anomalies we're trying to detect;
--   * the trailing window explicitly excludes the current day
--     (ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) so a day never influences
--     its own baseline;
--   * a baseline is only reported once a FULL 7-row trailing window exists
--     (row_num > 7) — a partial window from only 1-6 prior days would be an
--     unreliable, noisy baseline, and this also keeps this SQL mart exactly
--     reproducible by the pure-Python reference implementation in
--     eventstream.analytics.anomalies (see tests/integration/test_pipeline.py);
--   * threshold 3.5 on the 0.6745-scaled MAD z-score is the standard
--     Iglewicz & Hoaglin robust-outlier rule.
-- UNPIVOT lets one window-function pipeline serve all four monitored
-- metrics instead of duplicating the CTE chain four times.
CREATE OR REPLACE TABLE mart_anomalies AS
WITH unpivoted AS (
    UNPIVOT (SELECT activity_date, active_wallets, transfer_count, total_volume, median_amount FROM mart_daily_metrics)
    ON active_wallets, transfer_count, total_volume, median_amount
    INTO NAME metric_name VALUE metric_value
),
numbered AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY metric_name ORDER BY activity_date) AS row_num
    FROM unpivoted
),
rolling AS (
    SELECT
        metric_name,
        activity_date,
        metric_value,
        row_num,
        CASE WHEN row_num > 7 THEN
            MEDIAN(metric_value) OVER (
                PARTITION BY metric_name ORDER BY activity_date
                ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
            )
        END AS rolling_median
    FROM numbered
),
deviations AS (
    SELECT *, ABS(metric_value - rolling_median) AS abs_dev FROM rolling
),
mad AS (
    SELECT
        *,
        CASE WHEN row_num > 7 THEN
            MEDIAN(abs_dev) OVER (
                PARTITION BY metric_name ORDER BY activity_date
                ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
            )
        END AS rolling_mad
    FROM deviations
)
SELECT
    metric_name,
    activity_date,
    metric_value,
    rolling_median,
    rolling_mad,
    CASE
        WHEN rolling_mad IS NULL OR rolling_mad = 0 THEN NULL
        ELSE 0.6745 * (metric_value - rolling_median) / rolling_mad
    END AS robust_z_score,
    CASE
        WHEN rolling_mad > 0
             AND ABS(0.6745 * (metric_value - rolling_median) / rolling_mad) > 3.5
        THEN TRUE
        ELSE FALSE
    END AS is_anomaly
FROM mad
ORDER BY metric_name, activity_date;
