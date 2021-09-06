from pyspark import SparkContext
import sys
import csv
import json

business_input = sys.argv[1]
review_input = sys.argv[2]

sc = SparkContext('local[*]', 'preprocessing')

# Read json file
business_rdd = sc.textFile(business_input)
review_rdd = sc.textFile(review_input)

review_rdd = review_rdd.map(lambda line: json.loads(line))\
                       .map(lambda line: (line["business_id"], line["user_id"]))

business_rdd = business_rdd.map(lambda line: json.loads(line))\
                           .filter(lambda line: line["state"]=='NV')\
                           .map(lambda line: (line["business_id"],line["state"]))

joint_rdd = business_rdd.join(review_rdd)

joint_rdd = joint_rdd.mapValues(lambda line: line[1])\
                     .map(lambda line: (line[1],line[0]))

#print("join: \n", joint_rdd.take(5))

lst = joint_rdd.collect()

header = ['user_id', 'business_id']
with open("/Users/haonanxu/Desktop/dsci553/Assignment_2/join.csv", 'w') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(header)
    csvwriter.writerows(lst)
