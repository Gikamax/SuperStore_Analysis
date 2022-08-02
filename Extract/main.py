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
df_order_information.drop_duplicates(subset=["Order ID"], keep="last", inplace=True) # Some Orders have partially been returned, keep last instance. 
df_orders.drop(["Order ID", "Order Date", "Order Priority"], axis=1, inplace=True) # Drop Columns that are in this table. 

# Customer
df_customer = df_orders[["Customer ID", "Customer Name", "Customer Segment", "Country", "Region", "State or Province", "City", "Postal Code"]]
df_customer.drop_duplicates(inplace=True)
df_orders.drop(["Customer Name", "Customer Segment", "Country", "Region", "State or Province", "City", "Postal Code"], axis =1 , inplace=True)
df_customer.drop_duplicates(subset=["Customer ID"], keep="last", inplace=True) # Customers moved, Kept last instance of moving. 

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

## Read in Data
# Prepare Database (Create Schema's if needed)
etl.prepare_database(db_connection_string, ["dwh", "dm"]) # prepare database. 

# Read in the Dimensions
etl.load_hub(df_manager, "Manager", "Manager ID", db_connection_string) # Manager
etl.load_hub(df_order_information, "Order", "Order ID", db_connection_string) # Order information
etl.load_hub(df_customer, "Customer", "Customer ID", db_connection_string) # Customer
etl.load_hub(df_product, "Product", "Product ID", db_connection_string) # Product
etl.load_hub(df_shipping, "Shipment", "Shipping ID", db_connection_string) # Shipping

# Read in the Fact(s)
df_x = df_sales.head()


