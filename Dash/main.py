from dash_bootstrap_components.themes import FLATLY
from dash import Dash
from dotenv import load_dotenv, find_dotenv
import os
from src.data.source import DataSource
from src.data.loader import load_data
from src.components.layout import create_layout

load_dotenv(find_dotenv())
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")


def main() -> None:
    data = load_data(CONNECTION_STRING)
    data = DataSource(data)
    app = Dash(__name__, external_stylesheets=[FLATLY])
    app.title = "SuperStore Dashboard"
    app.layout = create_layout(app, data)
    app.run()


if __name__ == "__main__":
    main()
