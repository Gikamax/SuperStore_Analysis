from dash import Dash, dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
import sqlalchemy as db
from src.data.source import DataSource

from src.data.loader import Dataschema
from . import ids, colorTheme

def render(app:Dash, source: DataSource) -> html.Div:
    @app.callback(
        Output(ids.LINE_CHART, "children"),
        [Input(ids.CATEGORY_DROPDOWN, "value"), Input(ids.SEGMENT_DROPDOWN, "value")]
    )
    def update_line_chart(categories: list[str], segments: list[str]) -> html.Div:
        filtered_source = source.filter(categories=categories, segments=segments)
        #data.query("`Product Category` in @categories and Month in @months")

        if not filtered_source.row_count:
            return html.Div("No data selected.")
        
        fig = px.line(filtered_source.prepare_data_for_linechart(), 
                    x = Dataschema.ORDER_DATE,
                    y = [Dataschema.SALES, Dataschema.PROFIT])
        fig.update_layout(
            paper_bgcolor= colorTheme.PAGE_BACKGROUND_COLOR,
            font_color = colorTheme.TEXT_COLOR
        )
        
        return html.Div(dcc.Graph(figure=fig), id=ids.LINE_CHART)

    return html.Div(id = ids.LINE_CHART)