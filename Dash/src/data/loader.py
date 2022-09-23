import pandas as pd
import sqlalchemy as db

class Dataschema:
    MONTH = "Month"
    CATEGORY = "Product Category"
    SALES = "Sales"

def load_data(connection_string:str, view:str) -> pd.DataFrame:
    def read_data(connection_string:str, view:str) -> pd.DataFrame:
        engine = db.create_engine(connection_string)

        with engine.connect() as connection:
            df = pd.read_sql_query(f'select * from dm."{view}"', con=connection)
        return df

    def prepare_data(dataframe:pd.DataFrame) -> pd.DataFrame: # Add sorting to Month
        """
        function for the barchart. 
        """
        return dataframe.groupby([Dataschema.MONTH, Dataschema.CATEGORY]).agg({Dataschema.SALES: "sum"}).reset_index()
    df = read_data(connection_string=connection_string, view=view)
    df = prepare_data(df)
    return df