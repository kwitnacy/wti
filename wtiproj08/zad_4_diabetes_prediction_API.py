from flask import Flask, request, abort
import json
import hashlib
from joblib import load, dump
from api_logic import API


model = load('diabetes_clf.joblib')
app = Flask(__name__)
api = API('diabetes_clf')


@app.route('/patient_record', methods=['POST'])
def get_patient_record():
	try:
		data = request.json
		return json.dumps(api.get_patient_data(data))
	except:
		abort(400)


@app.route('/patient_prediction/<patiend_id>', methods=['GET'])
def get_patient_prediction(patiend_id):
	try:
		return api.get_prediction(patiend_id)
	except:
		return 'No such patient', 404



@app.route('/model', methods=['PUT'])
def update_model():
	data = request.json
		
	if api.update(data['filename'] + '.csv'):

		print(data)
		return 'wrong name - no such file'

	return 'OK', 200


@app.route('/models', methods=['PUT'])
def update_models():
	data = request.json
		
	if api.update_joblib(data['filename'] + '.joblib'):

		print(data)
		return 'wrong name - no such file'

	return 'OK', 200


if __name__ == '__main__':
    app.run()


