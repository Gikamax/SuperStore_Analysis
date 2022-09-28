from dash import Dash, html
import pandas as pd

from src.data.source import DataSource

from . import category_dropdown, bar_chart, month_dropdown

def create_layout(app:Dash, source:DataSource) -> html.Div:
    return html.Div(
        className= "app-div",
        children=[
            html.H1(app.title), 
            html.Hr(),
            html.Div(
                className="dropdown-container",
                children=[
                    category_dropdown.render(app, source),
                    month_dropdown.render(app, source)
                ]
            ),
            bar_chart.render(app, source)
        ]
    )