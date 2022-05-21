from mrjob.job import MRJob
from sklearn.metrics import mean_squared_error
import pickle


class MapReduceInference(MRJob):
    def mapper(self, _, line):
        currentExample = line.split(",")
        carrier = str((currentExample[0][1:])[:-1])
        yield carrier, [carrier, str(currentExample[1]), str(currentExample[2]), str(currentExample[3]), str(currentExample[4]), str(currentExample[5])]
    
    def reducer(self, key, values):
        yield key, self.modelPredict(key, values)

    def modelPredict(self, key, examples):
        indepVarValues = []
        depVarValues = []
        for example in examples:
            indepVarValues.append([float(example[1]), float(example[3]), float(example[4]), float(example[5])])
            depVarValues.append(int(example[2]))
        modelFile = "C:/Users/Mazen/Desktop/big data/Project/MapReduce/Models/" + key + "_model.sav"
        model = pickle.load(open(modelFile, 'rb'))
        predictions = model.predict(indepVarValues)
        predictionsFile = open("C:/Users/Mazen/Desktop/big data/Project/MapReduce/Predictions/" + key + "_predictions.txt", 'w')
        for prediction in predictions:
            predictionsFile.write(str(prediction) + "\n")
        predictionsFile.close()
        return "(RMSE: " + str(mean_squared_error(depVarValues, predictions, squared=False)) + ")"


if __name__ == '__main__':
    MapReduceInference.run()