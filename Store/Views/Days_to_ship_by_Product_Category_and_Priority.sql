-- Days to Ship by Product Category and Piority
CREATE VIEW dm."Days_to_ship_by_Product_Category_and_Priority"
AS
SELECT Product."Product Category"
	,_Order."Order Priority"
	,ROUND(CAST(Min(DATE_PART('day', Ship."Ship Date" - _Order."Order Date")) AS NUMERIC), 2) AS "Min Days to Ship"
	,ROUND(CAST(AVG(DATE_PART('day', Ship."Ship Date" - _Order."Order Date")) AS NUMERIC), 2) AS "AVG Days to Ship"
	,ROUND(CAST(MAX(DATE_PART('day', Ship."Ship Date" - _Order."Order Date")) AS NUMERIC), 2) AS "Max Days to Ship"
FROM dm."Sales" AS Sales
LEFT JOIN dm."Order" AS _Order ON _Order."Order ID" = Sales."Order ID"
LEFT JOIN dm."Shipment" AS Ship ON Ship."Shipping ID" = Sales."Shipping ID"
LEFT JOIN dm."Product" AS Product ON Product."Product ID" = Sales."Product ID"
GROUP BY Product."Product Category"
	,_Order."Order Priority"
ORDER BY Product."Product Category"
	,_Order."Order Priority" DESC