import pickle
import pandas as pd
import numpy as np
from sklearn import preprocessing
from sklearn.metrics import classification_report


def preProcessing(data):
    # drop irrelevent/unneeded features in classification:
    data.drop(['FL_DATE', 'OP_CARRIER', 'OP_CARRIER_FL_NUM',
              'ORIGIN', 'DEST', 'DIVERTED', 'Day', 'Year', 'Month'], axis=1, inplace=True)

    status = np.zeros(data.shape[0])
    status[data["ARR_DELAY"] > 0] = 1
    # these won't be used in the model -> labels
    data.drop(['ARR_DELAY', 'DEP_DELAY', 'CANCELLED'], axis=1, inplace=True)

    # Normalize all features:
    x = data.values  # returns a numpy array
    min_max_scaler = preprocessing.MinMaxScaler()
    x_scaled = min_max_scaler.fit_transform(x)
    data = pd.DataFrame(x_scaled, columns=data.columns)
    # load feat_selction_model
    modelFile = "C:/Collage/Bigdata/Project/Airline_Delay_Cancellation_Detection/classification/feat_selction_model.sav"
    model = pickle.load(open(modelFile, 'rb'))

    return model.transform(data.values), status


def predict(data):
    X, y = preProcessing(data)
    print('Dataset shape after preprocessing: ', X.shape)
    # load model
    modelFile = "C:/Collage/Bigdata/Project/Airline_Delay_Cancellation_Detection/classification/classification_model.sav"
    model = pickle.load(open(modelFile, 'rb'))
    # predict
    predictions = model.predict(X)
    predictionsFile = open(
        "C:/Collage/Bigdata/Project/Airline_Delay_Cancellation_Detection/classification/classification_predictions.txt", 'w')
    for prediction in predictions:
        if prediction == 0:
            predictionsFile.write("On-time\n")
        else:
            predictionsFile.write("Delayed\n")
    predictionsFile.close()

    print("Classification Report: " + classification_report(y, predictions))


if __name__ == "__main__":
    # read input csv file
    column_names = ['FL_DATE', 'OP_CARRIER', 'OP_CARRIER_FL_NUM', 'ORIGIN', 'DEST',
                    'CRS_DEP_TIME', 'DEP_TIME', 'DEP_DELAY', 'TAXI_OUT', 'WHEELS_OFF',
                    'WHEELS_ON', 'TAXI_IN', 'CRS_ARR_TIME', 'ARR_TIME', 'ARR_DELAY',
                    'CANCELLED', 'DIVERTED', 'CRS_ELAPSED_TIME', 'ACTUAL_ELAPSED_TIME',
                    'AIR_TIME', 'DISTANCE', 'CARRIER_DELAY', 'WEATHER_DELAY', 'NAS_DELAY',
                    'SECURITY_DELAY', 'LATE_AIRCRAFT_DELAY', 'Day', 'Year', 'Month']
    '''
    ['CRS_DEP_TIME', 'DEP_TIME', 'TAXI_OUT', 'WHEELS_OFF', 'WHEELS_ON',
       'TAXI_IN', 'CRS_ARR_TIME', 'ARR_TIME', 'CRS_ELAPSED_TIME',
       'ACTUAL_ELAPSED_TIME', 'AIR_TIME', 'DISTANCE', 'CARRIER_DELAY',
       'WEATHER_DELAY', 'NAS_DELAY', 'SECURITY_DELAY', 'LATE_AIRCRAFT_DELAY']
    '''
    data = pd.read_csv("Dataset/mapred_vis_data.csv", names=column_names)
    print('Dataset shape before preprocessing: ', data.shape)
    predict(data)
