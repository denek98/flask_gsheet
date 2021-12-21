from flask import current_app as app
from .google_sheet.gsheet_module import Gsheet


@app.route('/')
def upload_data_to_sheet():
	gsheet_object = Gsheet()
	df_from_input = gsheet_object.dataframe_from_input # Method to read from google sheet
	gsheet_object.upload_new_data_to_sheet(df_from_input) # Method to upload to google sheet
	return ("Success")

