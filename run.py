from app import create_app
from flask_apscheduler import APScheduler

app = create_app()
scheduler = APScheduler()
scheduler.init_app(app)

if __name__ == "__main__":
    # scheduler.start() # this doesn't work on app startup in docker
    app.run(debug=True, port=8080)
