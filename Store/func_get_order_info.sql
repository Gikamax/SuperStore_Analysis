-- Function to retrieve Order information
CREATE OR REPLACE FUNCTION dwh.get_order_info(
	Order_ID integer
)
RETURNS TABLE(
	"Element" text,
	"Status" varchar
)
LANGUAGE plpgsql
as $$
begin
	return query
			with cte as (
select 
	_Order."Order ID"
	,_Order."Order Date"::date
	,_Order."Order Priority"
	,ROUND(SUM(Sales."Sales")) as "Sales"
	,ROUND(SUM(Sales."Profit")) as "Profit"
	,Customer."Customer Name"
	,Customer."Customer Segment"
	,MAX(Ship."Ship Date")::date as "Ship Date"
from
dwh."SAT_Order" as _Order
LEFT JOIN dwh."LSAT_Sales" as Sales on Sales."Order ID" = _Order."Order ID"
LEFT JOIN dwh."SAT_Customer" as Customer on Customer."Customer ID" = Sales."Customer ID"
LEFT JOIN dwh."SAT_Shipment" as Ship on Ship."Shipping ID" = Sales."Shipping ID"
where _Order."Order ID" = Order_ID
GROUP BY 	_Order."Order ID",_Order."Order Date",_Order."Order Priority",Customer."Customer Name",Customer."Customer Segment"
), 
_result as (
select 'Order ID' as "Element", "Order ID"::varchar as "Status", 1 as "Sort" from cte
UNION
select 'Order Date', "Order Date"::varchar, 2 from cte
UNION
select 'Order Priority', "Order Priority"::varchar, 3 from cte
UNION
select 'Customer Name', "Customer Name"::varchar, 4 from cte
UNION
select 'Customer Segment', "Customer Segment"::varchar, 5 from cte
UNION
select 'Ship Date', "Ship Date"::varchar, 6 from cte
UNION
select 'Sales', "Sales"::varchar, 7 from cte
UNION
select 'Profit', "Profit"::varchar, 8 from cte
)
select _result."Element", _result."Status" from _result order by "Sort";

END;$$