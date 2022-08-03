-- Get best 3 customer per segment in terms of profit per month
with
  base_table as (
    select
      Customer."Customer ID",
      Customer."Customer Name",
      Customer."Customer Segment",
      date_part('month', _Order."Order Date") as Month_order,
      to_char(_Order."Order Date", 'MON') as Month_abrv,
      SUM(Sales."Sales") AS Sales,
      SUM(Sales."Profit") AS Profit
    from
      dwh."LSAT_Sales" AS Sales -- CHANGE TO VIEW
      LEFT JOIN dwh."SAT_Customer" as Customer on Customer."Customer ID" = Sales."Customer ID" -- CHANGE TO VIEW
      LEFT JOIN dwh."SAT_Order" as _Order on _Order."Order ID" = Sales."Order ID" -- CHANGE TO VIEW
    GROUP BY
      Customer."Customer ID",
      Customer."Customer Name",
      Customer."Customer Segment",
      date_part('month', _Order."Order Date"),
      to_char(_Order."Order Date", 'MON')
  ),
  best_3_customer_per_segment_and_month as (
    select
      *
    from
      (
        select
          "Customer ID",
          "Customer Name",
          "Customer Segment",
          Month_abrv,
          row_number() over (
            PARTITION BY
              base_table.Month_abrv,
              base_table."Customer Segment"
            ORDER BY
              Profit DESC
          ) as customer_rank,
          sales,
          profit
        from
          base_table
      ) ranks
    where
      customer_rank <= 3
  ),
  total_per_month_and_segment as (
    select
      base_table.Month_abrv as "Month",
      base_table."Customer Segment" as "Segment",
      ROUND(
        SUM(base_table.Profit) OVER (
          PARTITION BY
            base_table.Month_abrv,
            base_table."Customer Segment"
        )
      ) as "Total Profit",
      ROUND(
        SUM(base_table.Sales) OVER (
          PARTITION BY
            base_table.Month_abrv,
            base_table."Customer Segment"
        )
      ) as "Total Sales"
    from
      base_table
  )
select
  distinct base_table.Month_order as "Month Order",
  base_table.Month_abrv as "Month",
  base_table."Customer Segment" as "Segment",
  total_per_month_and_segment."Total Profit",
  total_per_month_and_segment."Total Sales",
  best_3_customer_per_segment_and_month."Customer Name",
  ROUND(best_3_customer_per_segment_and_month.profit) as "Profit per Customer",
  ROUND(
    (
      best_3_customer_per_segment_and_month.profit / total_per_month_and_segment."Total Profit"
    ) * 100
  ) as "Percentage of Total Profit",
  ROUND(best_3_customer_per_segment_and_month.sales) as "Sales per Customer",
  ROUND(
    (
      best_3_customer_per_segment_and_month.sales / total_per_month_and_segment."Total Sales"
    ) * 100
  ) as "Percentage of Total Sales"
from
  base_table
  LEFT JOIN total_per_month_and_segment on total_per_month_and_segment."Month" = base_table.Month_abrv
  and total_per_month_and_segment."Segment" = base_table."Customer Segment"
  LEFT JOIN best_3_customer_per_segment_and_month ON base_table."Customer ID" = best_3_customer_per_segment_and_month."Customer ID"
  and best_3_customer_per_segment_and_month."Customer Segment" = base_table."Customer Segment"
  and best_3_customer_per_segment_and_month.Month_abrv = base_table.Month_abrv
WHERE
  best_3_customer_per_segment_and_month.profit is not NULL
  and best_3_customer_per_segment_and_month.sales is not NULL
ORDER BY
  base_table.Month_order,
  "Segment",
  "Profit per Customer" desc,
  "Sales per Customer" desc
