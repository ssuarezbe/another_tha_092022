-- 1. What were the top 5 most placed order class in the last month of the data set?
-- SOLUTION Logic:
-- ASSUMPTION: The last month was 08 from year 2020
-- 1. Filter all the orders in that are NOT EMPTY or NULL, in the month and year that we want to analyze
--    NOTE: The EMPTY rows can be filtered at the ingestion STAGE.
-- 2. Group by order_class and count number of rows
-- 3. Order by rows count and get 5 top rows
-- 
-- SQL DB: Sqlite3
SELECT month_orders.order_class, count(month_orders.order_class) as order_cnt 
FROM
(
    SELECT order_time, order_class
    FROM procedure_order
    WHERE
    strftime('%m', order_time) IN ('08')
    AND
    strftime('%Y', order_time) IN ('2020')
    AND 
    order_class IS NOT NULL
    AND 
    LENGTH(order_class) > 0
) AS month_orders
GROUP BY month_orders.order_class 
ORDER BY count(month_orders.order_class) DESC
LIMIT 5
;
-- 2. On average, how many procedure orders were placed per patient within 24 hours
-- of their admit time by month in the last year of the dataset?
--
-- https://www.hl7.org/fhir/encounter.html
-- https://learnsql.com/cookbook/how-to-calculate-the-difference-between-two-timestamps-in-sqlite/
-- SOLUTION Logic:
-- ASSUMPTION: The last month was 08 from year 2020
-- 1. JOIN procedure and patient table by encounter_id to get the procedured per patient
-- 2. Filter the procedure that where placed within 24 hours of their admit time
-- 3. Calculate month and year of each procedure and group by patient_id to get the procedures count
-- 3. Calculate the AVG metric group by month and year
-- 
-- SQL DB: Sqlite3
SELECT
AVG(patient_procedures) as avg_procedures,
procedure_month,
procedure_year
FROM
(
    SELECT 
    pa.id as patient_id,
    count(*) as patient_procedures,
    strftime('%m', order_time) as procedure_month,
    strftime('%Y', order_time) as procedure_year
    FROM procedure_order as pr 
    JOIN patient_data as pa 
    ON pa.encounter_id = pr.encounter_id
    WHERE 
    JULIANDAY(pr.order_time) - JULIANDAY(pa.admit_time) <= 1.0
    GROUP BY patient_id
)
GROUP BY procedure_month, procedure_year;
-- 3. On average, how many orders were placed per patient within 24 hours of their
-- admit split by admit on a weekend vs. weekday by year
--
-- https://www.techonthenet.com/sqlite/functions/strftime.php
-- SOLUTION Logic:
-- ASSUMPTION: The last month was 08 from year 2020
-- 1. JOIN procedure and patient table by encounter_id to get the procedured per patient
-- 2. Filter the procedure that where placed within 24 hours of their admit time
-- 3. Calculate if the day is weekend or week day and group by patient_id to get the procedures count
-- 4. Group by admition_year, admition_week_cls and get the average
-- 
-- SQL DB: Sqlite3
SELECT
AVG(patient_procedures) as avg_procedures,
admition_week_cls,
admition_year
FROM
(
    SELECT 
    pa.id as patient_id,
    count(*) as patient_procedures,
    strftime('%w', pa.admit_time) as admition_week_day,
    strftime('%Y', pa.admit_time) as admition_year,
    CASE strftime('%w', pa.admit_time) 
        WHEN '0' THEN 'weekend'
        WHEN '6' THEN 'weekend' 
        ELSE 'weekday' 
    END admition_week_cls
    FROM procedure_order as pr 
    JOIN patient_data as pa 
    ON pa.encounter_id = pr.encounter_id
    WHERE 
    JULIANDAY(pr.order_time) - JULIANDAY(pa.admit_time) <= 1.0
    GROUP BY patient_id
)
GROUP BY admition_week_cls, admition_year;
-- 4. How would you visualize the result in question 3? Who would you expect your
-- audience to be? Why do you think it might be useful? You can either create a
-- visualization or tell us how you want to visualize this data.

-- The audience will be people from finance in hospital, personal that handle the required inventory for performing the procedures, HR personal that need to understand nurses or doctors capacity .

-- I'll visualize this data as a bar-time series with a data table. The data table allow a drill down operation by procedure.
