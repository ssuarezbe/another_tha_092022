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
-- SOLUTION Logic:
-- ASSUMPTION: The last month was 08 from year 2020
-- 1. Filter all rows that:
--    1.1 procedure_name is NOT EMPTY or NULL
--    1.2 procedure_name is NOT EMPTY or NULL
-- 
-- SQL DB: Sqlite3

-- 3. On average, how many orders were placed per patient within 24 hours of their
-- admit split by admit on a weekend vs. weekday by year

-- 4. How would you visualize the result in question 3? Who would you expect your
-- audience to be? Why do you think it might be useful? You can either create a
-- visualization or tell us how you want to visualize this data.
