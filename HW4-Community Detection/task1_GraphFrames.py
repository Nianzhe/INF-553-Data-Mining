from graphframes import *
from pyspark import SparkContext
from pyspark.sql import SQLContext
import time 
import sys

start_time = time.time()
input_path = sys.argv[1]
output_path = sys.argv[2]

sc = SparkContext()
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)
text = sc.textFile(input_path)

rdd = text.map(lambda x : tuple(x.split(" ")))

edge_list1 = rdd.map(lambda x : (x[1],x[0])).collect()
edge_list2 = rdd.map(lambda x : (x[0],x[1])).collect()
edge_list = list(set(edge_list1).union(set(edge_list2)))
edges = sqlContext.createDataFrame(edge_list,["src","dst"])

vertice_list1= rdd.map(lambda x : (x[0],)).distinct().collect()
vertice_list2= rdd.map(lambda x : (x[1],)).distinct().collect()
vertice_list = list(set(vertice_list1).union(set(vertice_list2)))
vertices = sqlContext.createDataFrame(vertice_list,["id"])

g = GraphFrame(vertices,edges)
result = g.labelPropagation(maxIter=5)
result = result.collect()
result_rdd = sc.parallelize(result)
group = result_rdd.map(lambda x: (x[1], [x[0]])).reduceByKey(lambda a,b: a+b).map(lambda x: (x[0],sorted(x[1]),len(x[1]))).collect()

group = sorted(group, key = lambda x: [x[2], x[1]])

with open(output_path,"w") as openfile:
    for i in range(len(group)):
        x = group[i][1]
        s = "','".join(x)
        s = "'" + s +"'"
        openfile.write("%s\n" % s)
sc.stop()
print("Duration: ", time.time()-start_time)
