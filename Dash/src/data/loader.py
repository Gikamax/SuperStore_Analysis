import pathlib
import pandas as pd
import sqlalchemy as db


class Dataschema:
    """
    Class to Store all Column Names for easy reference throughout the project.
    """
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


def load_data(connection_string: str) -> pd.DataFrame:
    """
    Reads Data from SQL Database, with predefined SQL Query. 
    """
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
