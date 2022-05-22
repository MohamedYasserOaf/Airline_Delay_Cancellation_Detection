import pickle
import pandas as pd
from sklearn import preprocessing


def preProcessing(data):
    # these won't be used in the model -> labels used in training (mainly aren't given)
    data.drop(['ARR_DELAY', 'DEP_DELAY', 'CANCELLED'], axis=1, inplace=True)
    # obvious irrelevent features: 'FL_DATE'
    data.drop('FL_DATE', axis=1, inplace=True)

    # Normalize all features:
    x = data.values  # returns a numpy array
    min_max_scaler = preprocessing.MinMaxScaler()
    x_scaled = min_max_scaler.fit_transform(x)
    data = pd.DataFrame(x_scaled, columns=data.columns)
    # load feat_selction_model
    modelFile = "C:/Collage/Bigdata/Project/Airline_Delay_Cancellation_Detection/classification/feat_selction_model.sav"
    model = pickle.load(open(modelFile, 'rb'))

    return model.transform(data)


def predict(data):
    X = preProcessing(data)
    # load model
    modelFile = "C:/Collage/Bigdata/Project/Airline_Delay_Cancellation_Detection/classification/classification_model.sav"
    model = pickle.load(open(modelFile, 'rb'))
    # predict
    predictions = model.predict(X)
    predictionsFile = open(
        "C:/Collage/Bigdata/Project/Airline_Delay_Cancellation_Detection/classification/classification_predictions.txt", 'w')
    for prediction in predictions:
        predictionsFile.write(str(prediction) + "\n")
    predictionsFile.close()


if __name__ == "__main__":
    # read input csv file
    data = None
    predict(data)
