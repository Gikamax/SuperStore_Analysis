from dash import Dash, html, dcc
from dash.dependencies import Input, Output
from src.data.source import DataSource

from src.data.loader import Dataschema
from . import ids, colorTheme
import pandas as pd 

def render(app:Dash, source: DataSource) -> html.Div:
    @app.callback(
        Output(ids.SEGMENT_DROPDOWN, "value"),
        Input(ids.SELECT_ALL_SEGMENTS_BUTTON, "n_clicks")
    )
    def select_all_segment(_:int) -> list[str]:
        return source.unique_segments

    return html.Div(
        children=[
            html.H6("Segment", style={'color': colorTheme.TEXT_COLOR}),
            dcc.Dropdown(
                id = ids.SEGMENT_DROPDOWN,
                options= [{"label": category, "value": category} for category in source.unique_segments],
                value=source.unique_segments,
                multi=True
            ),
            html.Button(
                className="segment-dropdown-button",
                children=["Select All"],
                id= ids.SELECT_ALL_SEGMENTS_BUTTON
            )
        ]
    )