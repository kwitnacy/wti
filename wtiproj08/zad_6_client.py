import requests
import pandas as pd
import time
import json
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn import metrics


def print_meta(r):
	print('-------------------')
	print('url: ', r.url)
	print('status code: ',  r.status_code)
	print('headers: ', r.headers)
	print('text: ',  r.text)
	try:
		print('body: ', r.request.body)
	except:
		pass
	print('r.headers: ', r.request.headers)
	print('-------------------')
	


url = 'http://127.0.0.1:5000'


X_test = pd.read_csv('X_test.csv')
y_test = pd.read_csv('y_test.csv')

model_time = str(hash(time.time()))
model_name = 'diabetes'

received_metadata = {}
received_metadata['ID'] = model_time
received_metadata['filename'] = model_name
req = requests.put(url + '/model', json=received_metadata)
model_dict = req.text
print_meta(req)

predictions = []
rows_iter = X_test.iterrows()
for row in rows_iter:
	row_dict = row[1].to_dict()

	req = requests.post(url + '/patient_record', json=row_dict)
	patient_dict = req.json()
	patient_ID = patient_dict['patient_ID']
	print_meta(req)

	req = requests.get(url + '/patient_prediction/' + patient_ID)
	prediction_dict = req.json()
	prediction = prediction_dict['prediction']
	print('prediction:', prediction)
	predictions.append(prediction)

	print_meta(req)

	
false_positive, true_positive, threshhold = metrics.roc_curve(y_test, predictions, pos_label=0)
AuROC = metrics.auc(false_positive, true_positive)

plt.figure()
plt.plot(false_positive, true_positive, color='blue', lw=2, label='ROC curve area %0.2f' % AuROC)
plt.xlim([0.0, 1.0])
plt.ylim([0.0, 1.05])
plt.xlabel('False positive')
plt.ylabel('True positive')
plt.title('ROC')
plt.legend(loc='lower right')
plt.savefig('wykres_klient.png')

