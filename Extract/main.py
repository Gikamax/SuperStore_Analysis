# Extract Script for the SuperStore Data

# Imports
import pandas as pd
import sqlalchemy as db
import os
from dotenv import load_dotenv
import ETL_Functions as etl

# suppress pandas warning
pd.set_option('mode.chained_assignment',None)

# Variables for connecting to Source and Database
load_dotenv() # load .env file 
db_connection_string = os.getenv("POSTGRES_DB") # Connection based on .env file. 
excel_path = os.path.dirname(__file__) + "/Data/SuperStoreUS_2015.xlsx"

## Retrieve all Excel sheets
df_orders = pd.read_excel(excel_path, "Orders")
df_returns = pd.read_excel(excel_path, "Returns")
df_users = pd.read_excel(excel_path, "Users")

## Get specific tables
# Manager
df_manager = df_users.copy() # Copy table
df_manager["Manager ID"] = range(1, len(df_manager)+ 1) # Create Unique Identifier. 
df_orders = df_orders.merge(df_manager[["Region", "Manager ID"]], on=["Region"]) # Merge back to df_orders to add Manager ID to orders. 

# Order Information
df_order_information = (df_orders[["Order ID", "Order Date", "Order Priority"]]
                        .join(df_returns, 
                        on=["Order ID"], 
                        rsuffix="_R")
                        .drop("Order ID_R", axis=1)) # create order information by joining two tables.
df_order_information["Status"].fillna("Delivered", inplace=True) # if Status NaN then no return
df_order_information.drop_duplicates(inplace=True) # Get Unique values. 
df_orders.drop(["Order ID", "Order Date", "Order Priority"], axis=1, inplace=True) # Drop Columns that are in this table. 

# Customer
df_customer = df_orders[["Customer ID", "Customer Name", "Customer Segment", "Country", "Region", "State or Province", "City", "Postal Code"]]
df_customer.drop_duplicates(inplace=True)
df_orders.drop(["Customer Name", "Customer Segment", "Country", "Region", "State or Province", "City", "Postal Code"], axis =1 , inplace=True)
# Figure out how to deal with Customer ID not being unique. (Customers have multiple Postal Codes)

# Product
df_product = df_orders[["Product Name", "Product Sub-Category", "Product Category", "Product Container", "Product Base Margin"]]
df_product.drop_duplicates(inplace=True)
df_product["Product ID"] = range(1, len(df_product) +1)
df_orders = (df_orders.merge(df_product,
            on = ["Product Name", "Product Sub-Category", "Product Category", "Product Container", "Product Base Margin"]
            )) # Merge Product back to add Product ID
df_orders.drop(["Product Name", "Product Sub-Category", "Product Category", "Product Container", "Product Base Margin"], axis =1, inplace=True)

# Shipping
df_shipping = df_orders[["Ship Mode", "Shipping Cost", "Ship Date"]]
df_shipping.drop_duplicates(inplace=True)
df_shipping["Shipping ID"] = range(1, len(df_shipping) +1)
df_orders = df_orders.merge(df_shipping, on = ["Ship Mode", "Shipping Cost", "Ship Date"])
df_orders.drop(["Ship Mode", "Shipping Cost", "Ship Date"], axis =1 , inplace=True)

# Sales
df_sales = df_orders.drop("Row ID", axis=1)

etl.prepare_database(db_connection_string, ["dwh", "dm"]) # prepare database. 

# Check if Customer ID in df_customer is unique // not unique some people moved
df_x = df_customer.groupby(["Customer ID"]).agg({"Postal Code": "count"})
print(df_x[df_x["Postal Code"] > 1])


## Test with Customer
# df_x = df_customer.head(10)

# etl.load_hub(df_x, "Customer", "Customer ID", db_connection_string)


# # Prepare dataframe
# df_x = etl.prepare_dataframe(df_x, "Product", "Product ID")
# #SET SAT and HUB
# hub_x = etl.create_hub(df_x, "Product", "Product ID")
# sat_x = etl.create_sat(df_x, "Product")
# # Check if Table exists
# table_exists = etl.check_if_table_exists("Product")

# if table_exists:
#     etl.insert_hub(hub_x, "Product")
#     etl.insert_sat(sat_x, "Product")
# else:
#     etl.create_hub_database(hub_x, "Product")
#     etl.create_sat_database(sat_x, "Product")
# print("Succes")




# print(df)

# engine = db.create_engine(db_connection_string)

# connection = engine.connect()
# metadata = db.MetaData()

# result = connection.execute("select * from information_schema.tables").fetchall()

# print(result)
