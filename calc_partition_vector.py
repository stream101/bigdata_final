#!/usr/bin/python

import sys
sys.path.append('/usr/lib/python2.6/dist-packages')
from mrjob.job import MRJob
import re
from sys import stderr
import re,pickle,base64,zlib
import csv

class MRWeather(MRJob):
    
    def __init__(self, *args, **kwargs):
        super(MRWeather, self).__init__(*args, **kwargs)
        self.valid_stations = {}
        self.station_id={}
    
    def configure_options(self):
        super(MRWeather,self).configure_options()
        self.add_file_option('--stationyear')
        self.add_file_option('--station_partition')
    
    def mapper_init(self):
        f= open(self.options.stationyear,'rb')
        reader = csv.reader(f)
        for line in reader:
            try:
                st = line[0]
                year = int(line[1])
                if st in self.valid_stations:
                    self.valid_stations[st].append(year)
                else:
                    self.valid_stations[st] = [year]
            except Exception as e: 
                pass
        f.close()
        
        f = open(self.options.station_partition, 'r')
        for line in f.readlines():
            words = line.split(',')
            words[1] = words[1].replace('\n', '')
            self.station_id[words[0]] = int(words[1])
        f.close()
    
    def mapper(self, _, line):
        self.increment_counter('MrJob Counters','mapper-all',1)
        try: 
            elements=line.split(',')
            year = int(elements[2])/10000
            mean_temp = float(elements[3])
            if str(elements[0]) in self.valid_stations and elements[0] != '999999' and str(elements[0]) in self.station_id:
                if year in self.valid_stations[str(elements[0])] and year >= 1930 and year <= 2008:
                    out = (int(self.station_id[elements[0]]), (year, mean_temp))
                    yield out
        except Exception, e: 
            stderr.write('Error in line:\n'+line)
            stderr.write(e)
            pass

    def reducer(self, station, info):
        
        self.increment_counter('MrJob Counters','reducer',1)
        info_list = list(info)
        mean_vector = [0] * 79
        mean_num = [0] * 79
        mean_sum = 0
        mean_cnt = 0
        num = len(info_list)
        
        for i in range(0, num):
            mean_vector[info_list[i][0]-1930] = mean_vector[info_list[i][0]-1930] + info_list[i][1]
            mean_num[info_list[i][0]-1930] = mean_num[info_list[i][0]-1930] + 1
            mean_sum = mean_sum + info_list[i][1]
            mean_cnt = mean_cnt + 1
        for i in range(0, 79):
            if mean_num[i] != 0:
                mean_vector[i] = mean_vector[i] / mean_num[i]
            else:
                mean_vector[i] = mean_sum / mean_cnt
        
        yield(station, mean_vector)   
        
if __name__ == '__main__':
    MRWeather.run()