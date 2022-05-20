from mrjob.job import MRJob
import pandas as pd
import numpy as np

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        line = line.split(',')
        row_df = pd.DataFrame([line])

    def reducer(self, key, values):
        yield key, 1


if __name__ == '__main__':
    MRWordFrequencyCount.run()