import warnings

def suppress_sqlalchemy_warnings():
    warnings.filterwarnings(
        "ignore",
        message="pandas only supports SQLAlchemy",
        category=UserWarning
    )
