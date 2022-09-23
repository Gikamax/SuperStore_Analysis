from src.components.layout import create_layout
from dash_bootstrap_components.themes import BOOTSTRAP
from dash import Dash, html



def main() -> None:
    app = Dash(external_stylesheets=[BOOTSTRAP])
    app.title = "SuperStore Dashboard"
    app.layout = create_layout(app)
    app.run()

if __name__ == "__main__":
    main()