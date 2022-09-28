from dash import Dash, html
import pandas as pd
import dash_bootstrap_components as dbc

from src.data.source import DataSource

from . import category_dropdown, bar_chart, month_dropdown, line_chart, segment_dropdown, colorTheme

def create_layout(app:Dash, source:DataSource) -> html.Div:
    return html.Div(
        style = {'backgroundColor': colorTheme.PAGE_BACKGROUND_COLOR},
        className= "app-div",
        children=[
            html.H1(app.title, style={'textAlign': 'center', 'color': colorTheme.TEXT_COLOR}), 
            html.Hr(),
            html.Div(
                children = [dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            [
                                html.H5("Filters", style={'textAlign': 'center', 'color': colorTheme.TEXT_COLOR}),
                                html.Hr(),
                                category_dropdown.render(app, source),
                                html.Hr(),
                                segment_dropdown.render(app, source)
                            ],
                            body=True,
                            style= {'backgroundColor': colorTheme.CARD_BACKGROUND_COLOR}
                            ),
                            md=4
                    ),
                    dbc.Col(children = [
                        html.H5("Sales and Profit by Order Date", style={'textAlign': 'center', 'color': colorTheme.TEXT_COLOR})
                        ,line_chart.render(app, source)
                        ]
                        , md=8)
                ],
                align= "center"
            )
            ]
            )
        #     html.Div(
        #         style = {'backgroundColor': colorTheme.BACKGROUND_COLOR},
        #         className="dropdown-container",
        #         children=[
        #             category_dropdown.render(app, source),
        #             segment_dropdown.render(app, source)
        #         ]
        #     ),
        #     html.H3("Sales and Profit by Order Date",
        #         style = {'textAlign': 'center', 'color': colorTheme.TEXT_COLOR}
        #     ),
        #     line_chart.render(app, source)
        ]
    )