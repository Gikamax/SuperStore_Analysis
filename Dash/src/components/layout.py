from dash import Dash, html
import pandas as pd
import dash_bootstrap_components as dbc

from src.data.source import DataSource

from . import category_dropdown, month_dropdown, line_chart, segment_dropdown, colorTheme, category_bar_chart, segment_bar_chart

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
                dbc.Col(children = [
                        html.H5("Sales and Profit by Order Date", style={'textAlign': 'center', 'color': colorTheme.TEXT_COLOR})
                        ,line_chart.render(app, source)
                        ]
                        , md=8),
                    dbc.Col(
                        dbc.Card(
                            [
                                html.H5("Filters", style={'textAlign': 'center', 'color': colorTheme.TEXT_COLOR}),
                                html.Hr(),
                                category_dropdown.render(app, source),
                                html.Hr(),
                                segment_dropdown.render(app, source), 
                                html.Hr(),
                                month_dropdown.render(app, source)
                            ],
                            body=True,
                            style= {'backgroundColor': colorTheme.CARD_BACKGROUND_COLOR}
                            ),
                            md=4
                    )
                ],
                align= "center"
            )
            ]
            ),
            html.Hr(),
            html.Div(
                children=[
                    dbc.Row(
                        [
                            dbc.Col(
                                children = [
                                html.H5("Sales per Category", style={'textAlign': 'center', 'color': colorTheme.TEXT_COLOR}),
                                category_bar_chart.render(app, source)
                                ],
                                md= 6,
                                align= "center"
                            ),
                            dbc.Col(
                                children = [
                                html.H5("Sales per Segment", style={'textAlign': 'center', 'color': colorTheme.TEXT_COLOR}),
                                segment_bar_chart.render(app, source)
                                ],
                                md= 6,
                                align= "center"
                            )
                        ]
                    )
                ]
            )
        ]
    )