from dash import Dash, html, dcc
from dash.dependencies import Input, Output
from src.data.source import DataSource
from . import ids, colorTheme

def render(app:Dash, source: DataSource) -> html.Div:
    @app.callback(
        Output(ids.MONTH_DROPDOWN, "value"),
        [Input(ids.SELECT_ALL_MONTHS_BUTTON, "n_clicks")]
    )
    def select_all_months(_:int) -> list[str]:
        return source.unique_months

    return html.Div(
        children=[
            html.H6("Month", style={'color': colorTheme.TEXT_COLOR}),
            dcc.Dropdown(
                id = ids.MONTH_DROPDOWN,
                options= [{"label": category, "value": category} for category in source.unique_months],
                value=source.unique_months,
                multi=True
            ),
            html.Button(
                className="months-dropdown-button",
                children=["Select All"],
                id= ids.SELECT_ALL_MONTHS_BUTTON
            )
        ]
    )