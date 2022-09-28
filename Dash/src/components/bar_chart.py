from dash import Dash, dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
import sqlalchemy as db
from src.data.source import DataSource

from src.data.loader import Dataschema
from . import ids

def render(app:Dash, source: DataSource) -> html.Div:
    @app.callback(
        Output(ids.BAR_CHART, "children"),
        [Input(ids.CATEGORY_DROPDOWN, "value"), Input(ids.MONTH_DROPDOWN, "value")]
    )
    def update_bar_chart(categories: list[str], months: list[str]) -> html.Div:
        filtered_source = source.filter(categories=categories, months=months)
        #data.query("`Product Category` in @categories and Month in @months")

        if not filtered_source.row_count:
            return html.Div("No data selected.")
        

        fig = px.bar(filtered_source.prepare_data_for_barchart(), x= Dataschema.ORDER_MONTH, y= Dataschema.SALES, color= Dataschema.CATEGORY, text_auto='.2s')
        return html.Div(dcc.Graph(figure=fig), id=ids.BAR_CHART)

    return html.Div(id = ids.BAR_CHART)
