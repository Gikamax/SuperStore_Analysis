from dash import Dash, dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
import sqlalchemy as db
from . import ids

def read_data(connection_string:str, view:str) -> pd.DataFrame:
    engine = db.create_engine(connection_string)

    with engine.connect() as connection:
        df = pd.read_sql_query(f'select * from dm."{view}"', con=connection)
    return df

def prepare_data(dataframe:pd.DataFrame) -> pd.DataFrame: # Add sorting to Month
    return dataframe.groupby(["Month", "Product Category"]).agg({"Sales": "sum"}).reset_index()

df = read_data("postgresql://username:password@localhost:5432/Superstore", "Monthly_Sales_Segment_and_Category")
df = prepare_data(df)

def render(app:Dash) -> html.Div:
    @app.callback(
        Output(ids.BAR_CHART, "children"),
        Input(ids.CATEGORY_DROPDOWN, "value")
    )
    def update_bar_chart(categories: list[str]) -> html.Div:
        filtered_data = df.query("`Product Category` in @categories")

        if filtered_data.shape[0] == 0:
            return html.Div("No data selected.")

        fig = px.bar(filtered_data, x="Month", y="Sales", color="Product Category", text="Product Category")
        return html.Div(dcc.Graph(figure=fig), id=ids.BAR_CHART)

    return html.Div(id = ids.BAR_CHART)
