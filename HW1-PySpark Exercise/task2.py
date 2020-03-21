from pyspark import SparkContext
import json
import sys
import time

inputfile = sys.argv[1]
outputfile = sys.argv[2]
n = int(sys.argv[3])

sc = SparkContext()
stringfile = sc.textFile(inputfile)

rdd1 = stringfile.map(json.loads)

def count_number_2(some_rdd):
    yield sum(1 for _ in some_rdd)

default_n = rdd1.getNumPartitions()
default_items = rdd1.mapPartitions(count_number_2).collect()
time1 = time.time()
default_F = rdd1.map(lambda x: (x['user_id'],int(x['review_count']))).top(10, key = lambda x:x[1])
time2 = time.time()
default_exe = time2-time1


rdd2 = rdd1.repartition(n)
customize_n = n
customize_items = rdd2.mapPartitions(count_number_2).collect()
time3 = time.time()
customize_F = rdd2.map(lambda x: (x['user_id'],int(x['review_count']))).top(10, key = lambda x:x[1])
time4 = time.time()
customize_exe = time4-time3

output = {"default":{
    "n_partition":default_n,
    "n_items":default_items,
    "exe_time":default_exe
},
    "customized": {
    "n_partition": customize_n,
    "n_items":customize_items,
    "exe_time":customize_exe
         },
    "explanation":"Running on my computer, with the number of partitions increasing, the running time will reduce."}

with open(outputfile, 'w') as js:
    json.dump(output, js, indent = 4)