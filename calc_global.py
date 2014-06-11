#!/usr/bin/python
"""
count the number of measurement for each year
"""
import sys
sys.path.append('/usr/lib/python2.6/dist-packages')
from mrjob.job import MRJob
import re
from sys import stderr
import re,pickle,base64,zlib

class MRWeather(MRJob):

    def mapper(self, _, line):
        
        self.increment_counter('MrJob Counters','mapper-all',1)
        elements=line.split(',')
        # 0: station 2: date 3: mean temp 11: visibility 17: max 18: min
        #out = (elements[0], (elements[3], elements[11], elements[17], elements[18]))
        year = int(elements[2])/10000
        mean_temp = float(elements[3])
        max_temp = float(elements[17].replace('*', ''))
        min_temp = float(elements[18].replace('*', ''))
        visible = float(elements[11])
        out = (year, (mean_temp, max_temp, min_temp, visible))
        #out = (year, min_temp)
        yield out

        
    def reducer(self, station, info):
        
        self.increment_counter('MrJob Counters','reducer',1)
        info_list = list(info)
        max_sum = 0.0
        min_sum = 0.0
        mean_sum = 0.0
        visible_sum = 0.0
        num = len(info_list)
        for i in range(0, num):
            mean_sum = mean_sum + info_list[i][0]
            max_sum = max_sum + info_list[i][1]
            min_sum = min_sum + info_list[i][2]
            visible_sum = visible_sum + info_list[i][3]
        yield(station, (mean_sum/num, max_sum/num, min_sum/num, visible_sum/num))     
     
        
if __name__ == '__main__':
    MRWeather.run()