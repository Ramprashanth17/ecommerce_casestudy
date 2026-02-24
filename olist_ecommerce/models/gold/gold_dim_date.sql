WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart = 'day',
        start_date = "cast('2016-01-01' as date)",
        end_date = "cast('2018-12-31' as date)"
    )}}
)

SELECT
{{ dbt_utils.generate_surrogate_key(['date_day'])}} as date_sk,
date_day,
YEAR(date_day) as year,
MONTH(date_day) as month,
DAYOFWEEK(date_day) as day_of_week,
CASE WHEN DAYOFWEEK(date_day) IN (0,6) THEN true
ELSE false
END AS is_weekend
FROM date_spine