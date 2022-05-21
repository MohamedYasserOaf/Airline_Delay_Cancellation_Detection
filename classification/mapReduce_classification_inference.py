from mrjob.job import MRJob
import pickle
from sklearn.metrics import classification_report

class MapReduceInference(MRJob):
    def mapper(self, _, line):
        currentExample = line.split(",")
        month = str(currentExample[4])
        yield month, [str(currentExample[0]), str(currentExample[1]), str(currentExample[2]), str(currentExample[3]), str(currentExample[5])]

    def reducer(self, key, values):
        yield key, self.modelPredict(key, values)

    def modelPredict(self, key, examples):
        indepVarValues = []
        depVarValues = []
        for example in examples:
            indepVarValues.append([float(example[0]), float(
                example[1]), float(example[2]), float(example[3])])
            depVarValues.append(int(example[4]))
        modelFile = "C:/Collage/Bigdata/Project/Airline_Delay_Cancellation_Detection/classification/Models/" + key + "_model.sav"
        model = pickle.load(open(modelFile, 'rb'))
        predictions = model.predict(indepVarValues)
        predictionsFile = open(
            "C:/Collage/Bigdata/Project/Airline_Delay_Cancellation_Detection/classification/Predictions/" + key + "_predictions.txt", 'w')
        for prediction in predictions:
            predictionsFile.write(str(prediction) + "\n")
        predictionsFile.close()
        return "Classification Report: " + classification_report(depVarValues, predictions)


if __name__ == '__main__':
    MapReduceInference.run()
