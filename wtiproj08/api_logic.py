from joblib import load, dump
import pandas as pd
import numpy as np
import hashlib
from sklearn.model_selection import train_test_split


class API():
	def __init__(self, model_name: str):
		self.model_name = model_name
		self.model = load(model_name + '.joblib')
		self.records = {}

	def get_patient_data(self, data: dict) -> dict:
		h = hashlib.sha224()
		h.update(bytes(str(data), encoding='utf-8'))
		key = h.digest().hex()
		
		self.records[key] = data
		
		return {'patient_ID': key}
	

	def get_prediction(self, patient_ID: str) -> dict:
		# print(pd.json_normalize(self.records[patient_ID]))
		return {
			'prediction': self.model.predict_proba(
				pd.json_normalize(self.records[patient_ID])
			)[0][0]
		}


	def update(self, model_name):
		print(model_name)
		data = pd.read_csv(model_name)

		X = data.loc[:, data.columns != 'Outcome']
		y = data['Outcome']
		
		self.model.fit(X, y)

		return 0

	def update_joblib(self, model_name):
		print(model_name)
		self.model = load(model_name)

if __name__ == "__main__":
	api = API('diabetes_clf')
	patient = {
		"Pregnancies": 1,
		"Glucose": 93,
		"BloodPressure": 70, 
		"SkinThickness": 31, 
		"Insulin": 0,
		"BMI": 30.4,
		"DiabetesPedigreeFunction": 0.315,
		"Age": 23
	}
	ret = api.get_patient_data(patient)
	print(ret)
	print(api.get_prediction(ret['patient_ID']))

	api.update_model()





