
from src.data.loader import load_data
from src.components.layout import create_layout
from dash_bootstrap_components.themes import BOOTSTRAP
from dash import Dash, html

CONNECTION_STRING = "postgresql://username:password@localhost:5432/Superstore"

def main() -> None:
    data = load_data(CONNECTION_STRING, "Monthly_Sales_Segment_and_Category")
    app = Dash(external_stylesheets=[BOOTSTRAP])
    app.title = "SuperStore Dashboard"
    app.layout = create_layout(app, data)
    app.run()

if __name__ == "__main__":
    main()