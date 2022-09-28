from json import load
import pandas as pd
import sqlalchemy as db
import pathlib

## Modify Loader to ensure all data is being loaded
## Summarize Data in each Chart. 

class Dataschema:
    ORDER_DATE = "Order Date"
    ORDER_MONTH = "Order Month"
    ORDER_PRIORITY = "Order Priority"
    STATUS = "Status"
    MANAGER = "Manager"
    REGION = "Region"
    CUSTOMER_NAME = "Customer Name"
    CUSTOMER_SEGMENT = "Customer Segment"
    COUNTRY = "Country"
    STATE = "State"
    CITY = "City"
    SHIP_MODE = "Ship Mode"
    SHIP_DATE = "Ship Date"
    SHIPPING_COST = "Shipping Cost"
    CATEGORY = "Category"
    SUB_CATEGORY = "Sub-Category"
    PRODUCT_NAME = "Product Name"
    BASE_MARGIN = "Product Base Margin"
    UNIT_PRICE = "Unit Price"
    QUANTITY = "Quantity ordered new"
    SALES = "Sales"
    DISCOUNT = "Discount"
    PROFIT = "Profit"



def load_data(connection_string:str) -> pd.DataFrame:
    engine = db.create_engine(connection_string)
    # Read in SQL Query
    with open(pathlib.Path(__file__).parent / "query.sql") as f:
        sql_query = f.read()

    # Read in Dataframe 
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sql_query, con=connection)
    # Add column Order Month
    df[Dataschema.ORDER_MONTH] = df[Dataschema.ORDER_DATE].dt.month.astype(str)
    return df

# df = load_data("postgresql://username:password@localhost:5432/Superstore")
# print(df[Dataschema.ORDER_MONTH].head())
# print(df[df[Dataschema.ORDER_MONTH].isin(['1','2'])])

# engine = db.create_engine("postgresql://username:password@localhost:5432/Superstore")
# with engine.connect() as con:
#     df = pd.read_sql_query(sql=sql_query, con=con)

# print(df.head())

# def load_data(connection_string:str, view:str) -> pd.DataFrame:
#     def read_data(connection_string:str, view:str) -> pd.DataFrame:
#         engine = db.create_engine(connection_string)

#         with engine.connect() as connection:
#             df = pd.read_sql_query(f'select * from dm."{view}"', con=connection)
#         return df

#     def prepare_data(dataframe:pd.DataFrame) -> pd.DataFrame: # Add sorting to Month
#         """
#         function for the barchart. 
#         """
#         return dataframe.groupby([Dataschema.MONTH, Dataschema.CATEGORY]).agg({Dataschema.SALES: "sum"}).reset_index()
#     df = read_data(connection_string=connection_string, view=view)
#     df = prepare_data(df)
#     return df