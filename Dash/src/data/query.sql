-- Raw Query for the Data Loader 
    SELECT
	_Order."Order Date"
	,_Order."Order Priority"
	,_Order."Status"
	,Manager."Manager"
	,Manager."Region"
	,Customer."Customer Name"
	,Customer."Customer Segment"
	,Customer."Country"
	,Customer."State or Province" as "State"
	,Customer."City"
	,Ship."Ship Mode"
	,Ship."Ship Date"
	,Ship."Shipping Cost"
	,Product."Product Category" as "Category"
	,Product."Product Sub-Category" as "Sub-Category"
	,Product."Product Name"
	,Product."Product Base Margin"
	,Sales."Unit Price"
	,Sales."Quantity ordered new"
	,Sales."Sales"
	,Sales."Discount"
	,Sales."Profit"
FROM
dm."Sales" as Sales
LEFT JOIN dm."Customer" as Customer on Sales."Customer ID" = Customer."Customer ID"
LEFT JOIN dm."Manager" as Manager on Sales."Manager ID" = Manager."Manager ID"
LEFT JOIN dm."Order" as _Order on _Order."Order ID" = Sales."Order ID"
LEFT JOIN dm."Product" as Product on Product."Product ID" = Sales."Product ID"
LEFT JOIN dm."Shipment" as Ship on Ship."Shipping ID" = Sales."Shipping ID"