from dash import Dash, html, dcc
from dash.dependencies import Input, Output

from src.data.loader import Dataschema
from . import ids
import pandas as pd 

def render(app:Dash, data: pd.DataFrame) -> html.Div:
    #all_categories = ["Technology", "Office Supplies", "Furniture"]
    all_categories = data[Dataschema.CATEGORY].unique().tolist()
    @app.callback(
        Output(ids.CATEGORY_DROPDOWN, "value"),
        Input(ids.SELECT_ALL_CATEGORIES_BUTTON, "n_clicks")
    )
    def select_all_categories(_:int) -> list[str]:
        return all_categories

    return html.Div(
        children=[
            html.H6("Category"),
            dcc.Dropdown(
                id = ids.CATEGORY_DROPDOWN,
                options= [{"label": category, "value": category} for category in all_categories],
                value=all_categories,
                multi=True
            ),
            html.Button(
                className="category-dropdown-button",
                children=["Select All"],
                id= ids.SELECT_ALL_CATEGORIES_BUTTON
            )
        ]
    )