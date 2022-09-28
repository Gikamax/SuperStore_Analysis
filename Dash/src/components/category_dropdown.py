from dash import Dash, html, dcc
from dash.dependencies import Input, Output
from src.data.source import DataSource

from src.data.loader import Dataschema
from . import ids, colorTheme
import pandas as pd 

def render(app:Dash, source: DataSource) -> html.Div:
    #all_categories = ["Technology", "Office Supplies", "Furniture"]
    @app.callback(
        Output(ids.CATEGORY_DROPDOWN, "value"),
        Input(ids.SELECT_ALL_CATEGORIES_BUTTON, "n_clicks")
    )
    def select_all_categories(_:int) -> list[str]:
        return source.unique_categories

    return html.Div(
        children=[
            html.H6("Category", style={'color': colorTheme.TEXT_COLOR}),
            dcc.Dropdown(
                id = ids.CATEGORY_DROPDOWN,
                options= [{"label": category, "value": category} for category in source.unique_categories],
                value=source.unique_categories,
                multi=True
            ),
            html.Button(
                className="category-dropdown-button",
                children=["Select All"],
                id= ids.SELECT_ALL_CATEGORIES_BUTTON
            )
        ]
    )