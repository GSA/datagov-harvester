from flask import Flask

from controller.config import Config


def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(config_class)

    from controller.dataset import bp as dataset_bp
    app.register_blueprint(dataset_bp, url_prefix='/dataset')

    from controller.source import bp as source_bp
    app.register_blueprint(source_bp, url_prefix='/source')

    from controller.job import bp as job_bp
    app.register_blueprint(job_bp, url_prefix='/job')

    @app.route('/')
    def index():
        return "Hello from Harvester process!"

    return app
