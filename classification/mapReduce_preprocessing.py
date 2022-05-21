from mrjob.job import MRJob
import pandas as pd
import numpy as np
from io import StringIO
import sys

from pyparsing import col



class MRWordFrequencyCount(MRJob):
        

    def mapper(self, _, line):
        self.cols = ["FL_DATE","OP_CARRIER","OP_CARRIER_FL_NUM","ORIGIN","DEST","CRS_DEP_TIME","DEP_TIME","DEP_DELAY","TAXI_OUT","WHEELS_OFF","WHEELS_ON","TAXI_IN","CRS_ARR_TIME","ARR_TIME","ARR_DELAY","CANCELLED","CANCELLATION_CODE","DIVERTED","CRS_ELAPSED_TIME","ACTUAL_ELAPSED_TIME","AIR_TIME","DISTANCE","CARRIER_DELAY","WEATHER_DELAY","NAS_DELAY","SECURITY_DELAY","LATE_AIRCRAFT_DELAY","Unnamed: 27"]
        self.delays = ["CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY", "SECURITY_DELAY" ,"LATE_AIRCRAFT_DELAY"]
        self.carriers_map = {
        'UA':'United Airlines',
        'AS':'Alaska Airlines',
        '9E':'Endeavor Air',
        'B6':'JetBlue Airways',
        'EV':'ExpressJet',
        'F9':'Frontier Airlines',
        'G4':'Allegiant Air',
        'HA':'Hawaiian Airlines',
        'MQ':'Envoy Air',
        'NK':'Spirit Airlines',
        'OH':'PSA Airlines',
        'OO':'SkyWest Airlines',
        'VX':'Virgin America',
        'WN':'Southwest Airlines',
        'YV':'Mesa Airline',
        'YX':'Republic Airways',
        'AA':'American Airlines',
        'DL':'Delta Airlines'
        }
        row = StringIO(line)
        row_df = pd.read_csv(row,sep=',',names=self.cols)
        row_df.drop("Unnamed: 27",inplace=True,axis=1)
        for delay in self.delays:
            row_df[delay]=row_df[delay].fillna(0)
        row_df.drop('CANCELLATION_CODE', inplace=True, axis=1) 
        row_df['OP_CARRIER'].replace(self.carriers_map, inplace=True)
        row_df["FL_DATE"] = pd.to_datetime(row_df.FL_DATE)
        row_df['Month'] = pd.DatetimeIndex(row_df['FL_DATE']).month
        row_df['Day'] = pd.DatetimeIndex(row_df['FL_DATE']).day
        row_df['Year'] = pd.DatetimeIndex(row_df['FL_DATE']).year

        new_cols = row_df.columns

        row_df_list = row_df.iloc[0].to_list()
        row_df_list = [str(val) for val in row_df_list]
        yield str(row_df['OP_CARRIER'][0]),row_df_list

    def reducer(self, key, values):
        total = []
        for value in values:
            total.append(value)
        yield key, 1

    



if __name__ == '__main__':
    MRWordFrequencyCount.run()