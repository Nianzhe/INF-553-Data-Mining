
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json
import datetime
import csv
import binascii

port_number = sys.argv[1]
output_filename = sys.argv[2]

sc = SparkContext()
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 10)
yelp = ssc.socketTextStream("localhost", int(port_number))

rdd = yelp.map(json.loads).map(lambda x:x["state"])
paras = [[14, 23]]
    
def append_sig(x,sig):
    num = int(binascii.hexlify(x.encode('utf8')),16)
    sig = list(sig) + [(num*p[0] + p[1])%200 for p in paras]
    sig = set(sig)
    return sig

def bloom(x):
    num = int(binascii.hexlify(x.encode('utf8')),16)
    b = [(num*p[0] + p[1])%200 for p in paras]
    return set(b)

with open(output_filename, "w") as openfile:
        writer = csv.writer(openfile)
        writer.writerow(["Time","FPR"])
openfile.close()


def bloom_filter(rdd):
    s = {}
    sig = []
    negtive = 0
    falsepos = 0
    for x in rdd.collect():
        detect = s.get(x,False)
        if detect == False:
            negtive += 1
            b = bloom(x)
            if b.issubset(set(sig)):
                falsepos += 1
        sig = append_sig(x,sig)
        s[x] = True
    if negtive == 0:
        r = 0
    else:
        r = falsepos/negtive
    t = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(output_filename, "a") as openfile:
        writer = csv.writer(openfile)
        writer.writerow([t,r])
    openfile.close()

rdd.foreachRDD(lambda rdd : bloom_filter(rdd))

ssc.start()
ssc.awaitTermination()