-- Monthly Sales/Profit by Segment and Product Category
CREATE VIEW dm."Monthly_Sales_Segment_and_Category" as 
SELECT TO_CHAR(_Order."Order Date", 'Mon') AS "Month"
	,Product."Product Category"
	,Customer."Customer Segment"
	,ROUND(CAST(SUM(Sales."Sales") AS NUMERIC), 2) AS "Sales"
	,ROUND(CAST(SUM(Sales."Profit") AS NUMERIC), 2) AS "Profit"
FROM dm."Sales" AS Sales
LEFT JOIN dm."Order" AS _Order ON _Order."Order ID" = Sales."Order ID"
LEFT JOIN dm."Customer" AS Customer ON Customer."Customer ID" = Sales."Customer ID"
LEFT JOIN dm."Product" AS Product ON Product."Product ID" = Sales."Product ID"
GROUP BY TO_CHAR(_Order."Order Date", 'Mon')
	,Product."Product Category"
	,Customer."Customer Segment"
	,DATE_PART('month', _Order."Order Date")
ORDER BY DATE_PART('month', _Order."Order Date")