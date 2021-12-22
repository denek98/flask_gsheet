from flask import Flask

def init_app():
	app = Flask(__name__)
	app.secret_key = 'Very_secret_secret_key'
	with app.app_context():
		from . import routes

		return app
