from application import init_app

app = init_app()

if __name__ == '__main__':
	#app.run('0.0.0.0:5000')
	app.run(debug=True)
