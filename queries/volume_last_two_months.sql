  SELECT DATE_TRUNC(DATE(revenue.date), MONTH) AS month
       , bacs_working_day AS working_day
       , SUM(revenue.amount) AS volume
    FROM `gc_data_warehouse.revenue` AS revenue
          LEFT JOIN `gc_data_warehouse.working_days` AS working_days
                    ON working_days.date = date_without_timestamp
   WHERE DATE(revenue.date) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 MONTH)
GROUP BY month
       , working_day
ORDER BY month
       , working_day
