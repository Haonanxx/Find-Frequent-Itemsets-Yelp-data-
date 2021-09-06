from pyspark.context import SparkContext
import json
import sys
import os
import itertools
from itertools import combinations
import math
import collections
from collections import Counter
import time



from pyspark.mllib.fpm import FPGrowth



sc = SparkContext('local[*]', 'task3')
start_time = time.time()
filter_threshold = int(sys.argv[1])
s_threshold = int(sys.argv[2])
input_file = sys.argv[3]
output_file = sys.argv[4]

input_rdd = sc.textFile(input_file)
header = input_rdd.first() #extract header
input_rdd = input_rdd.filter(lambda line: line != header)#filter out header

#groupby user_id and filter out qualified user_id/baskets
baskets = input_rdd.map(lambda line: line.split(','))\
         .map(lambda line: [line[0], [line[1]]]) \
         .reduceByKey(lambda a,b: a+b)\
         .filter(lambda line: len(line[1]) > filter_threshold)\
         .map(lambda line: list(set(line[1])))

fullDataSize = baskets.count()
min_support = s_threshold / fullDataSize

#data = [["a", "b", "c"], ["a", "b", "d", "e"], ["a", "c", "e"], ["a", "c", "f"]]
#rdd = sc.parallelize(data, 2)
model = FPGrowth.train(baskets,min_support,100)
result = sorted(model.freqItemsets().collect(), key=lambda x:(len(x),x))

task3_lst = []
for item in result:
    temp = item[0]
    temp.sort()
    task3_lst.append(temp)

task3_lst.sort(key=lambda x: (len(x),x))
    
with open('task2_res', 'r') as f:
    lines = f.read().splitlines()

f.close()

task2_lst = []
for item in lines:
    temp = item[1:-1].split("', '")
    task2_lst.append(temp)

#Reference: https://www.geeksforgeeks.org/python-count-of-common-elements-in-the-lists/
num_intersect = sum(x == y for x, y in zip(task2_lst, task3_lst))

with open(output_file, 'w') as fp:
    fp.write("Task2,{0}\n".format(len(task2_lst)))
    fp.write("Task3,{0}\n".format(len(task3_lst)))
    fp.write("Intersection,{0}".format(num_intersect))

fp.close()
    
print('----------------------------------')
print()
print("Checkpoint! ")
print()
print('----------------------------------')
