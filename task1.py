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

#count for baskets
def countPairs(baskets_lst,freq_items):

    pairs=[]
    #append item both in frequent item set and basket
    for basket in baskets_lst:
        basket[1].sort(reverse=True)
        for item in basket[1]:
            if item in freq_items:
                for item_2 in basket[1]:
                    if item_2 in freq_items and item_2 < item:
                        pairs.append((item,item_2))
    pair_counter = Counter(pairs)

    return pair_counter

#filter out items which are less than partition_threshold
#Reference: https://stackoverflow.com/questions/44734912/filter-out-elements-that-occur-less-times-than-a-minimum-threshold
def support_filter(counter,threshold):
    for key, cnts in list(counter.items()):
        if cnts < threshold:
            del counter[key]
    return counter

# Pass 1
def a_prioir(partition) : #algorithm apply to each partition,each partition contains a few baskets
    baskets_lst = list(partition)
    items=[]

    chunkSize = len(baskets_lst)
    localSupport =  math.ceil((chunkSize/fullDataSize) * s_threshold) # Piazza @139 discussion

    #count each every single
    for basket in baskets_lst:
        for item in basket[1]:
            items.append(item)
    item_counter = Counter(items)
    #remove single that occurs less than s/n tims from item_counter
    item_counter = support_filter(item_counter,localSupport)


    single_candidate = dict(item_counter)# counts included
    single_candidate = list(single_candidate)# counts not included

    candidates=[]
    candidates.append(single_candidate)
    #candidates.append(pair_candidate)

    n = 2 #iter number for combinations

    # generating candidates for size >= 3, stop when candidates list is empty
    while True :
        lst=[]
        for basket in baskets_lst:
            basket_set = set(basket[1])
            freq = set(single_candidate)
            #items in basket appear in L_k
            temp = basket_set.intersection(freq)
            construct = combinations(sorted(temp),n)
            for i in construct:
                lst.append(tuple(i))

        candidate = list(support_filter(Counter(lst),localSupport))
        if len(candidate) == 0:# No more qualified candidates
            break
        candidates.append(candidate)
        #updating single_candidate to higher size
        #Reference: https://stackoverflow.com/questions/10632839/transform-list-of-tuples-into-a-flat-list-or-a-matrix/35228431
        single_candidate = set(sum(candidate, ()))#flatten a list of tuples
        n=n+1


    # each partition will return candidates for all possible size
    yield candidates


sc = SparkContext('local[*]', 'task1')

start_time = time.time()

case = sys.argv[1]# 1 or 2
s_threshold = sys.argv[2]
input_csv = sys.argv[3]
output_txt = sys.argv[4]
s_threshold = int(s_threshold)
num_partition = 3
partition_threshold = math.floor(s_threshold / num_partition)


#read file
input_rdd = sc.textFile(input_csv)
header = input_rdd.first() #extract header
input_rdd = input_rdd.filter(lambda line: line != header)#filter out header

#format raw data into key value pair baskets


baskets_old = input_rdd.map(lambda line: line.split(','))

if case == '1':
    baskets = baskets_old.map(lambda line: [line[0], [line[1]]]) \
                         .reduceByKey(lambda a,b: a+b)
if case == '2':
    baskets = baskets_old.map(lambda line: [line[1], [line[0]]]) \
                           .reduceByKey(lambda a,b: a+b)

fullDataSize = baskets.count()


# partition with a specific number and apply each partition with a_prioir
pass_1 = baskets.mapPartitions(a_prioir)
#p=pass_1.collect()
#eliminate duplicates of candidates,save candidates as list for second pass
pass_1 = pass_1.flatMap(lambda pair: pair) \
                   .flatMap(lambda pair: pair) \
                   .map(lambda pair: (pair,1)) \
                   .reduceByKey(lambda a,b: a+b)\
                   .map(lambda pair: pair[0]) # counts are not important, will count all in 2nd pass

candidates_lst =  pass_1.collect()

#make candidates_lst print friendly
print_can_lst=[]
for can in candidates_lst:
    if isinstance(can, str): # make singleton tuples
        temp = tuple([can])
        print_can_lst.append(temp)
    else:
        print_can_lst.append(can)
#Reference: https://stackoverflow.com/questions/4659524/how-to-sort-by-length-of-string-followed-by-alphabetical-order
print_can_lst.sort(key=(lambda can: (len(can), can)))
#print(print_can_lst)

print('----------------------------------')
print()
print("Checkpoint! ")
print()
print('----------------------------------')

#---------------------------------------------------------------------------
#----------------------PASS 2 Below-----------------------------------------
#----------------------PASS 2 Below-----------------------------------------
#----------------------PASS 2 Below-----------------------------------------
#---------------------------------------------------------------------------

# for each baskets, for each candidate from candidates_lst,
# if candidate is a subset of the basket
# produce that candidate with count = 1
def second_pass(partition):
    baskets_lst = list(partition)
    for basket in baskets_lst:
        for candi in candidates_lst:
            if set(candi).issubset(set(basket[1])) or candi in basket[1]:
                yield (candi,1)

#Pass 2
# count the final frequents based on candidates list,
# groupby and add up all 1s
# if count >= s_threshold, output
final_frequent = baskets.mapPartitions(second_pass)\
                .reduceByKey(lambda a,b: a+b) \
                .filter(lambda tuple: tuple[1] >= s_threshold) \

frequent_lst = final_frequent.map(lambda tuple: tuple[0]).collect()

#print(frequent_lst)

print_freq_lst=[]
for freq in frequent_lst:
    if isinstance(freq, str): # make singleton tuples
        temp = tuple([freq])
        print_freq_lst.append(temp)
    else:
        print_freq_lst.append(freq)
#Reference: https://stackoverflow.com/questions/4659524/how-to-sort-by-length-of-string-followed-by-alphabetical-order
print_freq_lst.sort(key=(lambda freq: (len(freq), freq)))

with open(output_txt, 'w') as fp:
    fp.write("Candidates:\n")
    curr_len = 1 # initial length
    temp_lst = [] # a list to save candidates with same length
    for item in print_can_lst:
        if len(item) > curr_len:
            curr_len += 1
            print_str = ','.join(temp_lst) # join all strings
            fp.write("{0}\n\n".format(print_str))
            temp_lst = [] # empty list

        if len(item) == 1:
            temp_lst.append(str(item).replace(',','')) # remove comma after the singleton
        else:
            temp_lst.append(str(item))

    # temp_lst with longest-length items has not printed yet
    print_str = ','.join(temp_lst) # join all strings
    fp.write("{0}\n\n".format(print_str))

    fp.write("Frequent Itemsets:\n")
    curr_len = 1 # initial length
    temp_lst = [] # a list to save candidates with same length
    for item in print_freq_lst:
        if len(item) > curr_len:
            curr_len += 1
            print_str = ','.join(temp_lst) # join all strings
            fp.write("{0}\n\n".format(print_str))
            temp_lst = [] # empty list

        if len(item) == 1:
            temp_lst.append(str(item).replace(',','')) # remove comma after the singleton
        else:
            temp_lst.append(str(item))
    # last element without extra newlines
    print_str = ','.join(temp_lst) # join all strings
    fp.write("{0}".format(print_str))
    
fp.close()

#print("Candidates: ",'\n',candidates_lst)
#print("Final Frequent: ",'\n',final_frequent.collect())
#print(baskets.collect())
print("Duration:%s" % (time.time() - start_time))

print('----------------------------------')
print()
print("Checkpoint! ")
print()
print('----------------------------------')
