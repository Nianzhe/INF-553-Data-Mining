from pyspark import SparkContext
import json
import sys
import time

inputfile1 = sys.argv[1]
inputfile2 = sys.argv[2]
outputfile1 = sys.argv[3]
outputfile2 = sys.argv[4]

sc = SparkContext()

text_business = sc.textFile(inputfile2)
text_review = sc.textFile(inputfile1)

business = text_business.map(json.loads)
review = text_review.map(json.loads)

rdd = business.map(lambda x:(x['business_id'], x['state'])).join(review.map(lambda x: (x['business_id'],x['stars'])))
newrdd = rdd.map(lambda x:( x[1][0], x[1][1])).groupByKey().mapValues(lambda x: sum(x)/len(x))

new = newrdd.sortBy(lambda x: (-x[1],x[0]))

time1 = time.time()
print(new.collect()[0:5])
time2 = time.time()
exe_1 = time2 - time1


time3 = time.time()
print(new.take(5))
time4 = time.time()
exe_2 = time4 - time3


answer_a = sorted(newrdd.collect(),key = lambda x: (-x[1],x[0]))
answer_title = [('state','stars')]+answer_a

with open(outputfile1,"w") as open_file:
    for i in answer_title:
        s = str(i[0])+','+str(i[1])+' '
        open_file.write('%s\n' % s)

output_json = {
    "m1":exe_1,
    "m2":exe_2,
    "explanation":"Firstly collect the tuples into list, then take the first 5 values will take more time than directly take the value in rdd"
}       

with open(outputfile2,"w") as js:
    json.dump(output_json, js, indent = 4)