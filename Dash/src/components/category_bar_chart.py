from dash import Dash, dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
from src.data.source import DataSource

from src.data.loader import Dataschema
from . import ids, colorTheme

def render(app:Dash, source: DataSource) -> html.Div:
    @app.callback(
        Output(ids.CATEGORY_BAR_CHART, "children"),
        [Input(ids.MONTH_DROPDOWN, "value"), Input(ids.CATEGORY_DROPDOWN, "value"), Input(ids.SEGMENT_DROPDOWN, "value")]
    )
    def update_bar_chart(months: list[str], categories: list[str], segments: list[str]) -> html.Div:
        filtered_source = source.filter( months=months, categories=categories, segments=segments)

        if not filtered_source.row_count:
            return html.Div("No data selected.")
        

        fig = px.bar(filtered_source.prepare_data_for_category_barchart(),
                    x= Dataschema.ORDER_MONTH, 
                    y= Dataschema.SALES,
                    color = Dataschema.CATEGORY,
                    barmode = "group",
                    text_auto='.2s')
        fig.update_layout(            
            paper_bgcolor= colorTheme.PAGE_BACKGROUND_COLOR,
            font_color = colorTheme.TEXT_COLOR)
        return html.Div(dcc.Graph(figure=fig), id=ids.CATEGORY_BAR_CHART)

    return html.Div(id = ids.CATEGORY_BAR_CHART)