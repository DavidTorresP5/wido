import os
import glob
import logging
import re
import datetime

import pandas as pd
from flask import Flask, render_template, request, redirect, flash, send_from_directory
from werkzeug.utils import secure_filename

from src import app2slack
from src import app2aws
from src import utils
from src.dataproc import Data

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s|%(name)s|%(levelname)s|%(message)s")
file_handler = logging.FileHandler("logs/fileinput.log")
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

UPLOAD_FOLDER = 'static/data'

app = Flask(__name__)
app.secret_key = "secret key"
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024


@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                          'favicon.ico',mimetype='image/vnd.microsoft.icon')


@app.route('/')
def upload_form():
	return render_template('upload.html')


@app.route('/', methods=['POST'])
def upload_file():

	"""
	Main html with the upload input and several text input for user to interact with the app
	"""

	if request.method == 'POST':

		# There isn't file...
		if 'file' not in request.files:
			msg = 'No file'
			logger.info(f"PROBLEM:{msg}")
			flash(msg)
			return redirect(request.url)


		ficheros = request.files['file']

		# Create a list of files for the case of a multiple file upload
		list_of_files = []
		for f in request.files.getlist('file'):
			list_of_files.append(f.filename)

		# Create separator a company variables based on user inputs
		sep = request.form['separator']
		company = request.form['company']
		token = "".join(re.findall(r'\d+',str(datetime.datetime.now())))

		# Filename without name
		if ficheros.filename == '':
			msg = 'File has no name'
			logger.info(f"PROBLEM:{msg}")
			flash(msg)
			return redirect(request.url)

		# Filename exists and extension is valid
		if ficheros and utils.allowed_file(ficheros.filename):

			"""
			*** TO DO ***
			- Function for both single & multiple files to avoid code repetition 
			"""

			if len(list_of_files) > 1:
				
				"""
				For multiple files:
				- Secure the names, save the raw files and load and concatenate them with our class Data
				"""

				list_dfs = []
				for fi in request.files.getlist('file'):
					fi.save(os.path.join(UPLOAD_FOLDER, secure_filename(fi.filename)))

					inst_df = Data(file=os.path.join(UPLOAD_FOLDER, secure_filename(fi.filename)),
											    sep=str(sep),
												encoding="utf-8")
					list_dfs.append(inst_df._df)
				df = pd.concat(list_dfs)

				filename_no_ext = os.path.splitext(ficheros.filename)[0]

				"""
				- Save the file with the common format, send the log and the slack notification and flash the html message.
				"""

				df.to_csv(os.path.join(UPLOAD_FOLDER,f"{token}_{company}_{filename_no_ext}.csv"),
							sep = "|",
							index = False,
							encoding="utf-8")

				# utils.create_rdata(os.path.join(UPLOAD_FOLDER,f"{company}_{filename_no_ext}_{token}.csv")

				msg = f'Files loaded: ({list_of_files})'
				logger.info(f"{msg} by {company}")

				# Try to send a notification to slack (except file doesn't exist)
				try:
					app2slack.send_message(f"{msg} by {company}", app2slack.webhook_url)
				
				except:
					"""
					*** TO DO ***
					- Add error logger
					"""
					pass

				flash(msg)
				return redirect('/')

			else:
				"""
				For a single file:
				- Secure the name, create a filename without extension variable, save the raw file and load it with our class Data
				"""
				filename = secure_filename(ficheros.filename)
				filename_no_ext = os.path.splitext(filename)[0]

				ficheros.save(os.path.join(UPLOAD_FOLDER, filename))
				inst_df = Data(file=os.path.join(UPLOAD_FOLDER, filename),
									sep=str(sep),
									encoding="utf-8")
				"""
				- Save the file with the common format, send the log and the slack notification and flash the html message.
				"""
				inst_df._df.to_csv(os.path.join(UPLOAD_FOLDER,f"{token}_{company}_{filename_no_ext}.csv"),
									sep = "|",
									index = False,
									encoding="utf-8")

				# utils.create_rdata(os.path.join(UPLOAD_FOLDER,f"{company}_{filename_no_ext}_{token}.csv")
				
				msg = f'File loaded: ({filename})'
				logger.info(f"{msg} by {company}")

				try:
					app2slack.send_message(f"{msg} by {company}", app2slack.webhook_url)

				except:
					pass

				flash(msg)
				return redirect('/')
		
		else:
			flash("File with a wrong extension")
			return redirect(request.url)



@app.route("/table")
def show_tables():

	"""
	Render /tabla html with a sample dataframe of the last file loaded
	"""

	data = Data(file = max(glob.glob(os.path.join(UPLOAD_FOLDER,'*')), key=os.path.getctime), 
					 sep="|", 
					 encoding = "utf-8")
    
	return render_template('table.html',
							tables=[data.show_sample(5).to_html(classes='datatable')],
							titles = ['na', 'Sampling table'])



@app.route("/stats")
def show_summary():

	"""
	Render /stats html with tables with information and data quality review of the last file loaded
	"""

	data = Data(file = max(glob.glob(os.path.join(UPLOAD_FOLDER,'*')), key=os.path.getctime), 
					 sep="|", 
					 encoding = "utf-8")
					 
	return render_template('stats.html',
							tables=[data.show_summary().to_html(classes='datatable'),
									data.show_num_cols_info().to_html(classes='datatable'),
									data.show_dataqual_info().to_html(classes='datatable')],
							titles = ['na', 'Review', 'Info with numeric columns',
							'Data quality'])

	

if __name__ == "__main__":
    app.run(debug=True)