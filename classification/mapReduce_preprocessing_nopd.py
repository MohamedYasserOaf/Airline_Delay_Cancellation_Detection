from mrjob.job import MRJob
from mrjob.step import MRStep
import pandas as pd
import numpy as np
from io import StringIO
from datetime import datetime

from sqlalchemy import false
from sympy import appellf1


class MRWordFrequencyCount(MRJob):
    

    def mapper(self, _, line):
        self.cols = {"FL_DATE":0,
        "OP_CARRIER":1,
        "OP_CARRIER_FL_NUM":2,
        "ORIGIN":3,
        "DEST":4,
        "CRS_DEP_TIME":5,
        "DEP_TIME":6,
        "DEP_DELAY":7,
        "TAXI_OUT":8,
        "WHEELS_OFF":9,
        "WHEELS_ON":10,
        "TAXI_IN":11,
        "CRS_ARR_TIME":12,
        "ARR_TIME":13,
        "ARR_DELAY":14,
        "CANCELLED":15,
        "CANCELLATION_CODE":16,
        "DIVERTED":17,
        "CRS_ELAPSED_TIME":18,
        "ACTUAL_ELAPSED_TIME":19,
        "AIR_TIME":20,
        "DISTANCE":21,
        "CARRIER_DELAY":22,
        "WEATHER_DELAY":23,
        "NAS_DELAY":24,
        "SECURITY_DELAY":25,
        "LATE_AIRCRAFT_DELAY":26,
        "Unnamed: 27":27}
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

        row = line.split(",")
        row = row[0:-1] #0,26
        for delay in self.delays:
            if row[self.cols[delay]] != 0 and row[self.cols[delay]] =='':
                row[self.cols[delay]]=0
        del row[self.cols["CANCELLATION_CODE"]]
        row[self.cols['OP_CARRIER']]=self.carriers_map[row[self.cols['OP_CARRIER']]]
        row[0] = datetime.strptime(row[0], "%Y-%m-%d").date()
        row.append(row[0].day)
        row.append(row[0].year)
        row.append(row[0].month)
        row[0]=str(row[0])

        yield str(row[-1]),row

    def reducer(self, key, values):
        total = []
        for value in values:
            app=True
            for data_item in value:
                if data_item=='':
                    app=False
                    break
            if app:
                total.append(value)
        yield key, len(total)



if __name__ == '__main__':
    MRWordFrequencyCount.run()