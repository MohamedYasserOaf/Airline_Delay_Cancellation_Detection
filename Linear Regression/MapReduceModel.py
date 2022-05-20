from mrjob.job import MRJob
import pandas as pd
from sklearn.linear_model import LinearRegression


class MapReduceModel(MRJob):
    def mapper(self, _, line):
        currentExample = line.split(",")
        carrier = str((currentExample[0][1:])[:-1])
        yield carrier, [carrier, str(currentExample[1]), str(currentExample[2]), str(currentExample[3]), str(currentExample[4]), str(currentExample[5])]
    
    def reducer(self, key, values):
        yield key, self.modelFit(values)

    def modelFit(self, examples):
        indepVarValues = []
        depVarValues = []
        for example in examples:
            indepVarValues.append([float(example[1]), float(example[3]), float(example[4]), float(example[5])])
            depVarValues.append(int(example[2]))
        model = LinearRegression().fit(indepVarValues, depVarValues)
        return model.score(indepVarValues, depVarValues)


if __name__ == '__main__':
    MapReduceModel.run()