from src.data.source import DataSource
from src.data.loader import load_data
from src.components.layout import create_layout
from dash_bootstrap_components.themes import FLATLY
import dash_bootstrap_components as dbc
from dash import Dash, html, register_page

CONNECTION_STRING = "postgresql://username:password@localhost:5432/Superstore"

def main() -> None:
    data = load_data(CONNECTION_STRING)
    data = DataSource(data)
    app = Dash(__name__,external_stylesheets=[FLATLY])
    app.title = "SuperStore Dashboard"
    app.layout = create_layout(app, data)
    app.run()

if __name__ == "__main__":
    main()