from pyspark import SparkContext, SparkConf
import time
import csv
import itertools as it
import sys


inputfile = sys.argv[3]
outputfile = sys.argv[4]
fil_th = int(sys.argv[1])
threshold = int(sys.argv[2])

start_time2 = time.time()

with open(inputfile,'r') as csv_load:
    reader = csv.reader(csv_load, delimiter=',',)
    new_list = []
    i =0
    for row in reader:
        if i >0:
            x= [row[0][:-4]+ row[0][-2:]+"-" + row[1],int(row[5])]
            new_list.append(x)
        i+=1
csv_load.close()
new_list = [['DATE-CUSTOMER_ID','PRODUCT_ID']]+new_list
with open('clean_data.csv', 'w') as writeFile:
    writer = csv.writer(writeFile)
    writer.writerows(new_list)
writeFile.close()


conf = SparkConf().setMaster("local[*]").set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf = conf)

t = sc.textFile('clean_data.csv')
header = sc.parallelize([t.first()])
t = t.subtract(header)
rdd2 = t.map(lambda x: x.split(","))

basket = rdd2.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda a,b: a+b).mapValues(set)

basket_filter = basket.filter(lambda x: len(x[1]) >fil_th)

num = basket_filter.getNumPartitions()
if threshold/num <=1 and threshold !=1:
    basket_filter = basket_filter.repartition(threshold-1)
    num_partition = basket_filter.getNumPartitions()
else:
    num_partition = num

num_partition = basket_filter.getNumPartitions()

def collect_basket(basket_rdd):
    l = []
    for i in basket_rdd:
        l.append(i[1])
    return l
def apriori2(basket_rdd): 
    basket = collect_basket(basket_rdd)
    s = threshold/num_partition
    
    ## For singleton
    canlist1=[]
    countdict1= {}
    for l in basket:
        for i in list(l):
            countdict1[i] = countdict1.get(i,0)+1 
    for x,count in countdict1.items():
        if count >= s:
            canlist1.append(x)
    
    ## For pairs
    canlist2 = []
    countdict2 = {}
    for cand2 in it.combinations(canlist1,2):
        for l in basket:
            if set(cand2).issubset(l):
                countdict2[cand2] = countdict2.get(cand2,0)+1
    for x,count in countdict2.items():
        if count >= s:
            x = sorted(x)
            canlist2.append(tuple(x))

    candidates = []
    for i in canlist1:
        candidates.append(i)
    for i in canlist2:
        candidates.append(i)

    ## For triples, etc.
    size = 3
    previous = canlist2
    while True:
        countdict = {}
        precurrentlist = []
        currentlist = []
        for i in it.combinations(previous,2):
            a = i[0]
            b = i[1]
            if len(set(a).difference(b))==1:
                cand = set(a).union(set(b))
                cand = tuple(sorted(cand))
                if cand in precurrentlist:
                    continue
                else:
                    sub_cand = it.combinations(cand,size-1)
                    if set(sub_cand).issubset(previous):
                        precurrentlist.append(cand)
        for cand in precurrentlist:
            for l in basket:
                if set(cand).issubset(l):
                    countdict[cand] = countdict.get(cand,0)+1
        for x,count in countdict.items():
            if count >= s:
                currentlist.append(x)
        if len(currentlist) == 0:
            break
        else:
            for i in currentlist:
                candidates.append(i)
            previous = currentlist
            size+=1 
    return candidates


im_candidates = basket_filter.mapPartitions(apriori2).distinct().collect()

def count_rdd(basket_rdd):
    basket = collect_basket(basket_rdd)
    count_sub = {}
    for cand in im_candidates:
        for itemlist in basket:
            if type(cand) is str and cand in list(itemlist):
                count_sub[cand] = count_sub.get(cand,0)+1
            elif set(cand).issubset(itemlist):
                count_sub[cand] = count_sub.get(cand,0)+1
    return count_sub.items()


result = basket_filter.mapPartitions(count_rdd).reduceByKey(lambda a,b : a+b).filter(lambda x: x[1] >= threshold).map(lambda x:x[0]).collect()


with open(outputfile,"w") as open_file:
    open_file.write("Candidates:\n")
    start = 1
    strlist = []
    t = "("
    for cand in im_candidates:
        if type(cand) is str:
            strlist.append(cand)
    strlist = sorted(strlist)
    for cand in strlist:
        if start ==1:
            t =  t+ "'"+ cand + "'" +")"
        elif type(cand) is str and start !=1:
            t = t +"," + "("+ "'"+ cand + "'" +")"
        start = start +1
    open_file.write('%s\n\n' % t)
    
    t_size = 2
    while True:
        sub_l = []
        for x in im_candidates:
            if len(x) == t_size and type(x) is tuple:
                x = str(x)
                sub_l.append(x)
        sub_l = sorted(sub_l)
        output_s = ",".join(sub_l)
        open_file.write('%s' % output_s)
        if len(sub_l) == 0:
            break       
        open_file.write("\n\n")
        t_size+=1 

    open_file.write("Frequent Items:\n")
    start2 = 1
    strlist2 = []
    t2 = "("
    for cand2 in result:
        if type(cand2) is str:
            strlist2.append(cand2)
    strlist2 = sorted(strlist2)
    for cand2 in strlist2:
        if start2 ==1:
            t2 = t2 + "'"+ cand2+ "'" +")"
        elif type(cand2) is str and start2 !=1:
            t2 = t2 +"," + "("+  "'"+cand2+ "'" +")"
        start2 = start2 +1
    open_file.write('%s\n\n' % t2)
    
    t_size2 = 2
    while True:
        sub_l2 = []
        for x2 in result:
            if len(x2) == t_size2 and type(x2) is tuple:
                x2 = str(x2)
                sub_l2.append(x2)
        sub_l2 = sorted(sub_l2)
        output_s2 = ",".join(sub_l2)
        open_file.write('%s' % output_s2)
        if len(sub_l2) == 0:
            break       
        open_file.write("\n\n")
        t_size2+=1 
sc.stop()
print("Duration:",time.time()- start_time2)