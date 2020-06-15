from sklearn.neural_network import MLPClassifier
from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV
from sklearn import metrics
from joblib import dump, load
import matplotlib.pyplot as plt
import zmq
import time
import json
import pandas as pd


def build_model(filename: str):
    diabetes = pd.read_csv('diabetes.csv')

    diabetes = pd.read_csv('diabetes.csv')
    X = diabetes.loc[:, diabetes.columns != 'Outcome']
    y = diabetes['Outcome']
    X_train, X_test, y_train, y_test = train_test_split(X, y)
    X_test.to_csv("X_test.csv", index=False)
    y_test.to_csv("y_test.csv", index=False)

    mlp = MLPClassifier()
    MLPClassifier_parameter_space = {
            'hidden_layer_sizes': [(50,50,50), (50,100,50), (100,)],
            'activation': ['tanh', 'relu'],
            'solver': ['sgd', 'adam'],
            'alpha': [0.0001, 0.05],
            'learning_rate': ['constant','adaptive'],
    }

    dev_MLPClassifier_parameter_space = {
            'hidden_layer_sizes': [(50)],
            'activation': ['tanh'],
            'solver': ['adam'],
            'alpha': [0.05],
            'learning_rate': ['adaptive'],
    }

    MLPClassifier_parameter_space = dev_MLPClassifier_parameter_space
    clf = GridSearchCV(mlp, MLPClassifier_parameter_space, n_jobs=1, cv=3)
    clf.fit(X_train, y_train)
    best_params = clf.best_params_

    clf = MLPClassifier(**best_params)
    clf.fit(X, y)
    clf.fit(X_train, y_train)
    model_name = filename
    dump(clf, model_name + '.joblib')
    clf = load(model_name + '.joblib')
    
    return filename  


port = str(5555)
context = zmq.Context()

socket = context.socket(zmq.PAIR)

socket.bind("tcp://*:%s" % port)

while True:
    print('Job executor is waiting for new job request')
    msg = socket.recv()
    print('Job executor got mess')
    msg_dict = json.loads(msg.decode('ascii'))
    print(msg_dict)
    msg_dict_value = msg_dict['job_ID']
    print(msg_dict_value)
    print('Job executor is starting')

    model_name = build_model('new_name_diabetes')

    print('Job completed')
    print('Job executor is about to report the completion of the hob back to the requester')

    msg_content = {}
    msg_content['job_ID'] = msg_dict_value
    msg_content['model_name'] = model_name

    msg_content_str = str(json.dumps(msg_content))
    msg_content_str = msg_content_str.encode('ascii')
    socket.send(msg_content_str)
