-- Function to return product overview based on Week Number
CREATE or REPLACE FUNCTION dm.Profit_per_Week(
	Week_num integer
)
RETURNS TABLE(
	"Product Category" text,
	"Product Sub-Category" text,
	"Sales" double precision,
	"Percentage of Total Sales" numeric,
	"Profit" double precision, 
	"Percentage of Total Profit" numeric
)
LANGUAGE plpgsql
as $$
begin
return query 
with base_table as (
select
	DATE_PART('week', _Order."Order Date") as "Week Number"
	,Product."Product Category"
	,Product."Product Sub-Category"
	,SUM(Sales."Sales") as "Sales"
	,SUM(Sales."Profit") as "Profit"
from dm."Sales" as Sales
LEFT JOIN dm."Order" as _Order on _Order."Order ID" = Sales."Order ID"
LEFT JOIN dm."Product" as Product on Product."Product ID" = Sales."Product ID"
GROUP BY DATE_PART('week', _Order."Order Date"), Product."Product Category", Product."Product Sub-Category"
), cumulative as (
select
	base_table."Week Number"
	,base_table."Product Category"
	,base_table."Product Sub-Category"
	,ROUND(sum(base_table."Sales") over (order by base_table."Week Number" asc, base_table."Product Category", base_table."Product Sub-Category" rows between unbounded preceding and current row )) as "Total Sales"
	,ROUND(sum(base_table."Profit") over (order by base_table."Week Number" asc, base_table."Product Category", base_table."Product Sub-Category" rows between unbounded preceding and current row )) as "Total Profit"
FROM base_table
), _result as (
SELECT
	base_table."Product Category"
	,base_table."Product Sub-Category"
	,ROUND(SUM(base_table."Sales")) as "Sales"
	,ROUND(CAST((SUM(base_table."Sales") / SUM(cumulative."Total Sales")) AS numeric) * 100 , 2) as "Percentage of Total Sales"
	,ROUND(SUM(base_table."Profit")) as "Profit"
	,ROUND(CAST((SUM(base_table."Profit") / SUM(cumulative."Total Profit")) AS numeric) * 100 , 2) as "Percentage of Total Profit"
FROM base_table
LEFT JOIN cumulative on cumulative."Product Category" = base_table."Product Category"
			and cumulative."Product Sub-Category" = base_table."Product Sub-Category"
			and cumulative."Week Number" = Week_num
GROUP BY
GROUPING SETS (
	(base_table."Week Number"),
	(base_table."Week Number", base_table."Product Category"),
	(base_table."Week Number", base_table."Product Category", base_table."Product Sub-Category")
)
HAVING base_table."Week Number" = Week_num
order by 1,2
)
select * from _result;

END;$$
