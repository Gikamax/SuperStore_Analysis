from dash import Dash, html, dcc
from dash.dependencies import Input, Output

from src.data.loader import Dataschema
from . import ids
import pandas as pd 

def render(app:Dash, data: pd.DataFrame) -> html.Div:
    all_months = data[Dataschema.MONTH].unique().tolist()
    unique_months = sorted(set(all_months))
    @app.callback(
        Output(ids.MONTH_DROPDOWN, "value"),
        [Input(ids.SELECT_ALL_MONTHS_BUTTON, "n_clicks")]
    )
    def select_all_months(_:int) -> list[str]:
        return unique_months
    # def update_months(categories: list[str], _: int) -> list[str]:
    #     filtered_data = data.query("`Product Category` in @categories")
    #     return sorted(set(filtered_data[Dataschema.MONTH].tolist()))

    return html.Div(
        children=[
            html.H6("Month"),
            dcc.Dropdown(
                id = ids.MONTH_DROPDOWN,
                options= [{"label": category, "value": category} for category in unique_months],
                value=unique_months,
                multi=True
            ),
            html.Button(
                className="months-dropdown-button",
                children=["Select All"],
                id= ids.SELECT_ALL_MONTHS_BUTTON
            )
        ]
    )