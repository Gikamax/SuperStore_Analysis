-- Stored procedure to Update dwh.SAT_Order with processing time and expected_processing time 
CREATE OR REPLACE PROCEDURE dwh.spUpdate_Order () LANGUAGE plpgsql AS $$

DECLARE 
cur_orders CURSOR
FOR
SELECT DISTINCT _Order."Order ID"
	,MAX(DATE_PART('day', Ship."Ship Date" - _Order."Order Date")) AS "Processing Time"
FROM dwh."SAT_Order" AS _Order
LEFT JOIN (
	SELECT "Order ID"
		,"Shipping ID"
	FROM dwh."LSAT_Sales"
	) AS Sales ON _Order."Order ID" = Sales."Order ID"
LEFT JOIN dwh."SAT_Shipment" AS Ship ON Ship."Shipping ID" = Sales."Shipping ID"
GROUP BY _Order."Order ID";
row_orders record;

cur_expected_time CURSOR
FOR
SELECT DISTINCT _Order."Order ID"
	,ROUND(AVG(DATE_PART('day', Ship."Ship Date" - _Order."Order Date")) OVER (
			PARTITION BY Customer."State or Province"
			,_Order."Order Priority"
			,Ship."Ship Mode" ORDER BY _Order."Order Date" ROWS BETWEEN 4 PRECEDING
					AND CURRENT ROW
			)) AS "X"
FROM (
	SELECT "Customer ID"
		,"Order ID"
		,"Shipping ID"
	FROM dwh."LSAT_Sales"
	) AS Sales
LEFT JOIN dwh."SAT_Customer" AS Customer ON Sales."Customer ID" = Customer."Customer ID"
LEFT JOIN dwh."SAT_Order" AS _Order ON Sales."Order ID" = _Order."Order ID"
LEFT JOIN dwh."SAT_Shipment" AS Ship ON Sales."Shipping ID" = Ship."Shipping ID";
row_expected_time record;

BEGIN
	-- Add columns to Sat_Order if not exists. 
	ALTER TABLE dwh."SAT_Order" 
	ADD COLUMN IF NOT EXISTS "Processing Time" INTEGER
	,ADD COLUMN IF NOT EXISTS "Expected Processing Time" INTEGER;
	
	-- Open the first cursor
	OPEN cur_orders;

	LOOP

	FETCH cur_orders INTO row_orders;

	EXIT when NOT found;

	UPDATE dwh."SAT_Order"
	SET "Processing Time" = row_orders."Processing Time"
	WHERE "Order ID" = row_orders."Order ID";
	END LOOP;
CLOSE cur_orders;

	open cur_expected_time; 
	
	LOOP
	
	FETCH cur_expected_time INTO row_expected_time;
	
	EXIT when NOT FOUND;
	
	UPDATE dwh."SAT_Order"
	SET "Expected Processing Time" = row_expected_time."X"
	WHERE "Order ID" = row_expected_time."Order ID";
	END LOOP;
CLOSE cur_expected_time;

END;$$;