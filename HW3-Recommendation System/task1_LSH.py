from pyspark import SparkContext, SparkConf
import itertools as it
import time
import sys

input_file = sys.argv[1]
similarity_method = sys.argv[2]
output_file = sys.argv[3]
start_time = time.time()

conf = SparkConf().setMaster("local[*]").set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

text = sc.textFile(input_file)
header = sc.parallelize([text.first()])
text = text.subtract(header)
rdd = text.map(lambda x : x.split(","))

user = rdd.map(lambda x : x[0]).distinct().collect()
business = rdd.map(lambda x : x[1]).distinct().collect()
user_length = len(user)
business_length = len(business)
userD = {}
busD = {}
busD_r = {}
for i, u in enumerate(user):
    userD[u] = i
for j, b in enumerate(business):
    busD[b] = j
for j, b in enumerate(business):
    busD_r[j] = b

if similarity_method == "jaccard":
    matrix_rdd = rdd.map(lambda x : (busD[x[1]],[userD[x[0]]])).reduceByKey(lambda a,b : a+b)
    matrix = matrix_rdd.collectAsMap()
    paras_jac = [[913, 901, 24593], [14, 23, 769], [1, 101, 193], [17, 91, 1543], \
                [387, 552, 98317], [11, 37, 3079], [2, 63, 97], [41, 67, 6151], \
                [91, 29, 12289], [3, 79, 53], [73, 803, 49157], [8, 119, 389],\
                [97,67,196613],[199,1423,393241],[269,29,786433],[2003,503,1572869],
                [2179,227,3145739],[787,139,6291469],[2683,61,12582917],[5081,73,25165843],\
                [5987,431,50331653],[7591,199,100663319],[10069,1117,201326611],[16349,1439,402653189],\
                [9781,3329,196613],[593,1423,393241],[997,29,786433],[3407,503,1572869]]

    def hash_jaccard(user_bins, para):
        a = para[0]
        b = para[1]
        p = para[2]
        l = min(((a*user_m + b)%p)%user_length for user_m in user_bins)
        return l
    signature_jac = matrix_rdd.map(lambda x : (x[0], [hash_jaccard(x[1],para) for para in paras_jac]))
    
    def seperate_jac(x):
        n = len(paras_jac)
        r = 2
        b = int(n/r)
        rt = []
        for i in range(b):
            item = ((i, tuple(x[1][i*r:(i+1)*r])), [x[0]])
            rt.append(item)
        return rt
    candidates_jac = signature_jac.flatMap(seperate_jac).reduceByKey(lambda a,b: a+b).filter(lambda x : len(x[1])> 1).map(lambda x: tuple(x[1])).distinct()
    
    def find_cand(cand):
        pairs = it.combinations(cand, 2)
        return list(pairs)
    def jaccard(l):
        a1 = matrix[l[0]]
        a2 = matrix[l[1]]
        union_set = set(a1).union(set(a2))
        inter_set = set(a1).intersection(set(a2))
        return (l[0], l[1],len(inter_set)/len(union_set))

    result_jac = candidates_jac.flatMap(find_cand).distinct().map(jaccard).filter(lambda x : x[2] >= 0.5).distinct().collect()
    result_jac = sorted(result_jac, key = lambda x : x[2], reverse = False)
    result_jac_w = []
    for x in result_jac:
        b1 = busD_r[int(x[0])]
        b2 = busD_r[int(x[1])]
        result_jac_w.append(tuple(sorted([b1,b2])+[str(x[2])]))

    result_jac_w = sorted(result_jac_w, key = lambda x : [x[0],x[1]])
    with open(output_file, "w") as openfile:
        openfile.write("business_id_1,business_id_2,similarity \n")
        for x in result_jac_w:
            i = ",".join([x[0],x[1],x[2]])
            openfile.write('%s\n' % i)
            
elif similarity_method == "cosine":
    matrix_rdd = rdd.map(lambda x : (busD[x[1]],[userD[x[0]]])).reduceByKey(lambda a,b : a+b)
    matrix = matrix_rdd.collectAsMap()
    paras_cos = [[913, 901, 24593], [42491, 769, 24593], [97, 101, 12289], [719, 91, 24593], \
                [387, 552, 98317], [317, 37, 98317], [523, 63, 49157], [41, 67, 6151],\
                [91, 29, 12289], [36341, 79, 12289], [73, 803, 6151], [6653, 119, 196613],\
                [97,67,196613],[199,1423,393241],[269,29,786433],[2003,503,1572869],
                [2179,227,3145739],[787,139,6291469],[2683,61,12582917],[5081,73,25165843]
                ]

    def hash_cos(user_bins, para):
        a = para[0]
        b = para[1]
        dot_value = sum([((a*i + b)%user_length - int(user_length/2)) for i in user_bins])
        if dot_value >= 0:
            l = 1
        else:
            l = 0
        return l

    signature_cos = matrix_rdd.map(lambda x : (x[0], [hash_cos(x[1],para) for para in paras_cos]))

    def seperate_cos(x):
        n = len(paras_cos)
        r = 4
        b = int(n/r)
        rt = []
        for i in range(b):
            item = ((i, tuple(x[1][i*r:(i+1)*r])), [x[0]])
            rt.append(item)
        return rt

    candidates_cos = signature_cos.flatMap(seperate_cos).reduceByKey(lambda a,b: a+b).filter(lambda x : len(x[1])> 1).map(lambda x: tuple(x[1])).distinct()
    def cosine(l):
        a1 = matrix[l[0]]
        a2 = matrix[l[1]]
        inter_set = set(a1).intersection(set(a2))
        similarity = len(inter_set)/((len(a1)*len(a2))**0.5)
        return (l[0],l[1], similarity)
    def find_cand(cand):
        pairs = it.combinations(cand, 2)
        return list(pairs)

    result_cos = candidates_cos.flatMap(find_cand).map(cosine).filter(lambda x : x[2] >= 0.5).distinct().collect()

    result_cos_w = []
    for x in result_cos:
        b1 = busD_r[int(x[0])]
        b2 = busD_r[int(x[1])]
        result_cos_w.append(tuple(sorted([b1,b2])+[str(x[2])]))
    result_cos_w = sorted(result_cos_w, key = lambda x : [x[0],x[1]])

    with open(output_file, "w") as openfile:
        openfile.write("business_id_1,business_id_2,similarity \n")
        for x in result_cos_w:
            i = ",".join([x[0],x[1],x[2]])
            openfile.write('%s\n' % i)

else:
    print( "Similarity method should be 'jaccard' or 'cosine'")
sc.stop()
print("Running time is: ", time.time()- start_time)