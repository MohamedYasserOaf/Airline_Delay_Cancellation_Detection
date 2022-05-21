from mrjob.job import MRJob
from sklearn.linear_model import LogisticRegression
import pickle


class MapReduceModel(MRJob):
    def mapper(self, _, line):
        currentExample = line.split(",")
        month = str(currentExample[4])
        yield month, [str(currentExample[0]), str(currentExample[1]), str(currentExample[2]), str(currentExample[3]), str(currentExample[5])]

    def reducer(self, key, values):
        yield key, self.modelFit(key, values)

    def modelFit(self, key, examples):
        indepVarValues = []
        depVarValues = []
        for example in examples:
            indepVarValues.append([float(example[0]), float(
                example[1]), float(example[2]), float(example[3])])
            depVarValues.append(int(example[4]))
        model = LogisticRegression(solver='lbfgs', max_iter=200, random_state=1).fit(
            indepVarValues, depVarValues)
        modelFile = "C:/Collage/Bigdata/Project/Airline_Delay_Cancellation_Detection/classification/Models/" + key + "_model.sav"
        pickle.dump(model, open(modelFile, "wb"))
        return "(R-squared: " + str(model.score(indepVarValues, depVarValues)) + ")"


if __name__ == '__main__':
    MapReduceModel.run()
