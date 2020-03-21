from pyspark import SparkContext
import json
import sys

inputfile = sys.argv[1]
outputfile = sys.argv[2]

sc = SparkContext()
stringfile = sc.textFile(inputfile)
rdd = stringfile.map(json.loads)

answer1 = rdd.count()
answer2 = rdd.map(lambda x : x['review_count']).mean()
answer3 = rdd.map(lambda x : (x['name'],1)).groupByKey().count()
answer4 = rdd.filter(lambda x : "2011" in x['yelping_since']).count()
rdd5 = rdd.map(lambda x: (x["name"],1)).reduceByKey(lambda a,b: a+b)
answer5 = rdd5.takeOrdered(10, key=lambda x: (-x[1],x[0]))
answer6 = rdd.map(lambda x: (x['user_id'],int(x['review_count']))).top(10, key = lambda x:x[1])

dic = {"total_users":answer1,\
          "avg_reviews":answer2,\
          "distinct_usernames":answer3,\
          "num_users": answer4,\
          "top10_popular_names":answer5,\
          "top10_most_reviews":answer6}

with open(outputfile, 'w') as fp:
   json.dump(dic, fp, indent = 4)

