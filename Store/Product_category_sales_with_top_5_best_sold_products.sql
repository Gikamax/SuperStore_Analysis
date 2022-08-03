-- Query to get Total Sales per Category and Top 5 best Sold products in Percentage of Category per Year
with
  base_table as (
    SELECT
      Product."Product Category",
      Product."Product ID",
      Product."Product Name",
      SUM(Sales."Sales") AS Sales
    FROM
      dwh."vwLSAT_Sales_Currents" as Sales
      LEFT JOIN dwh."vwSAT_Product_Currents" as Product on Sales."Product ID" = Product."Product ID"
    GROUP BY
      Product."Product Category",
      Product."Product ID",
      Product."Product Name"
  ),
  top_five_products_per_category as(
    select
      "Product ID"
    FROM
      (
        select
          "Product ID",
          row_number() over (
            PARTITION BY
              "Product Category"
            order by
              Sales desc
          ) as "Product Rank"
        FROM
          base_table
      ) ranks
    WHERE
      "Product Rank" <= 5
  ),
  percentage_per_product_id as (
    select
      base_table."Product ID",
      ROUND(
        cast(
          (
            (
              Sales / total_per_category."Total Sales per Category"
            ) * 100
          ) as numeric
        ),
        2
      ) as "Percentage of Category"
    FROM
      base_table
      INNER JOIN (
        SELECT
          "Product Category",
          SUM(Sales) as "Total Sales per Category"
        FROM
          base_table
        GROUP BY
          "Product Category"
      ) as total_per_category on base_table."Product Category" = total_per_category."Product Category"
    WHERE
      base_table."Product ID" in (
        select
          *
        from
          top_five_products_per_category
      )
  )
select
  "Product Category",
  ROUND(
    sum(Sales) OVER (
      PARTITION BY
        "Product Category"
    )
  ) as "Total Sales per Category",
  "Product Name",
  percentage_per_product_id."Percentage of Category"
from
  base_table
  LEFT JOIN percentage_per_product_id on percentage_per_product_id."Product ID" = base_table."Product ID"
  WHERE percentage_per_product_id."Percentage of Category" IS NOT NULL
ORDER BY
  "Total Sales per Category" desc,
  "Percentage of Category" desc