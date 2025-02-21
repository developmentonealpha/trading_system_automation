from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from services.data_lake.config import Config

db = SQLAlchemy()
migrate = Migrate()

def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    db.init_app(app)
    migrate.init_app(app, db)
    from services.data_lake.routes import data_lake_bp
    app.register_blueprint(data_lake_bp, url_prefix="/api")

    # Import models to register them with SQLAlchemy
    from services.data_lake import models  # Add this line

    return app



