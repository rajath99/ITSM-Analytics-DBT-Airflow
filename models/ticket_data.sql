-- models/ticket_data.sql
WITH cleaned_data AS (
    SELECT DISTINCT
        ticket_id,
        created_date,
        category,
        priority,
        assigned_group,
        resolution_time,
        status
    FROM {{ ref('ticket_data') }}  -- Ensure this references the correct source table
    WHERE created_date IS NOT NULL
),
date_extracted AS (
    SELECT
        *,
        EXTRACT(YEAR FROM created_date) AS year,
        EXTRACT(MONTH FROM created_date) AS month,
        EXTRACT(DAY FROM created_date) AS day
    FROM cleaned_data
),
average_resolution_time AS (
    SELECT
        category,
        priority,
        AVG(resolution_time) AS avg_resolution_time
    FROM date_extracted
    GROUP BY category, priority
),
closure_rate AS (
    SELECT
        assigned_group,
        COUNT(*) FILTER (WHERE status = 'Closed')::FLOAT / COUNT(*) AS closure_rate
    FROM date_extracted
    GROUP BY assigned_group
),
monthly_summary AS (
    SELECT
        year,
        month,
        COUNT(*) AS ticket_count,
        AVG(resolution_time) AS avg_resolution_time,
        COUNT(*) FILTER (WHERE status = 'Closed')::FLOAT / COUNT(*) AS closure_rate
    FROM date_extracted
    GROUP BY year, month
)
SELECT * FROM monthly_summary;