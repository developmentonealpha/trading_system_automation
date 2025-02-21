import os

from flask_migrate import Migrate
# from services.backend_api.app import create_app as create_backend_app
# from services.broker_auth.auth_controller import create_app as create_auth_app
# from services.data_lake.app import create_app as create_data_lake_app

from services.data_lake.app import create_app as create_data_lake_app, db


apps = {
    # "backend": create_backend_app(),
    # "auth": create_auth_app(),
    "data_lake": create_data_lake_app(),
}

migrate = Migrate()

def get_app(service="backend"):
    """Retrieve the requested Flask app."""
    if service in apps:
        return apps[service]
    else:
        raise ValueError(f"Unknown service: {service}")

if __name__ == "__main__":
    # service = os.getenv("FLASK_SERVICE", "backend")  # Default to backend
    service = os.getenv("FLASK_SERVICE", "data_lake")  # Default to data_lake

    app = get_app(service)
    migrate.init_app(app, db)
    app.run(host="0.0.0.0", port=int(os.getenv(f"{service.upper()}_PORT", 5000)), debug=True)
