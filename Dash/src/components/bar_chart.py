from dash import Dash, dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
import sqlalchemy as db

from src.data.loader import Dataschema
from . import ids

def render(app:Dash, data: pd.DataFrame) -> html.Div:
    @app.callback(
        Output(ids.BAR_CHART, "children"),
        [Input(ids.CATEGORY_DROPDOWN, "value"), Input(ids.MONTH_DROPDOWN, "value")]
    )
    def update_bar_chart(categories: list[str], months: list[str]) -> html.Div:
        filtered_data = data.query("`Product Category` in @categories and Month in @months")

        if filtered_data.shape[0] == 0:
            return html.Div("No data selected.")
        

        fig = px.bar(filtered_data, x= Dataschema.MONTH, y= Dataschema.SALES, color= Dataschema.CATEGORY, text_auto='.2s')
        return html.Div(dcc.Graph(figure=fig), id=ids.BAR_CHART)

    return html.Div(id = ids.BAR_CHART)
