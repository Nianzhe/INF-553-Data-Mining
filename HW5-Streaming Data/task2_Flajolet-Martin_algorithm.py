import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json
import datetime
import csv
import binascii
import statistics
import random

port_number = sys.argv[1]
output_filename = sys.argv[2]

sc = SparkContext()
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 5)
yelp = ssc.socketTextStream("localhost", int(port_number)).window(30,10)

rdd = yelp.map(json.loads).map(lambda x:x["city"])

m = 500
#a = [24,128,256,64,16]
#b = [16,64,180,120,32]
ps = [53,97,193,389,769] # Source: https://planetmath.org/goodhashtableprimes
paras = []
random.seed(256)
for i in range(5):
    a = random.choice(range(m))
    b = random.choice(range(m))
    p = ps[i]
    paras.append([a,b,p])

def count_zeros(x):
    num = 0
    i = len(x) -1
    while i>0:
        if x[i] == '1':
            break
        elif i == len(x)-1 and x[i] == '0':
            num +=1
        elif x[i] == '0' and x[i+1] == '0':
            num+=1
        i = i-1 
    return num

with open(output_filename, "w") as openfile:
        writer = csv.writer(openfile)
        writer.writerow(["Time","Ground Truth","Estimation"])
openfile.close()

def find_cities(rdd):
    truth = rdd.distinct().count()
    rdd = rdd.map(lambda x: int(binascii.hexlify(x.encode('utf8')),16))
    result = []
    for i in range(len(paras)):
        para = paras[i]
        result.append(rdd.map(lambda x : ((para[0]*x + para[1])% para[2]) % m).map(lambda x : bin(x)).map(lambda x : count_zeros(x)).max())
    estimates = [2**r for r in result]
    # estimates = [[2**r for r in result[0:2]],[2**r for r in result[2:5]]]      
    # estimate = [statistics.mean(x) for x in estimates]
    e = statistics.mean(estimates)
    # e = int((estimate[int(len(estimate)/2)] + estimate[int(len(estimate)/2)+1])/2)
    t = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(output_filename, "a") as openfile:
        writer = csv.writer(openfile)
        writer.writerow([t,truth,int(e)])
    openfile.close()

rdd.foreachRDD(lambda rdd : find_cities(rdd))

ssc.start()
ssc.awaitTermination()