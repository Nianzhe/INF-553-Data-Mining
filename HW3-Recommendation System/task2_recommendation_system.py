from pyspark import SparkContext, SparkConf
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
import time
import itertools as it
import sys

train_path = sys.argv[1]
test_path  =  sys.argv[2]
case_id = int(sys.argv[3])
output_path = sys.argv[4]
start_time = time.time()

conf = SparkConf().setMaster("local[*]").set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

text_train = sc.textFile(train_path)
header = sc.parallelize([text_train.first()])
rdd_train = text_train.subtract(header)
rdd_train = rdd_train.map(lambda x : x.split(","))

text_test = sc.textFile(test_path)
text_test = text_test.subtract(sc.parallelize([text_test.first()]))
rdd_test = text_test.map(lambda x : x.split(","))

test_key = rdd_test.map(lambda x : (x[0], x[1])).map(lambda x :(x, None))
train = rdd_train.map(lambda x : ((x[0], x[1]),float(x[2]))).subtractByKey(test_key)
test_truth = rdd_test.map(lambda x : ((x[0], x[1]),float(x[2])))

user_train = train.map(lambda x : x[0][0]).distinct().collect()
business_train = train.map(lambda x : x[0][1]).distinct().collect()
user_test = rdd_test.map(lambda x : x[0]).distinct().collect()
business_test = rdd_test.map(lambda x : x[1]).distinct().collect()

user = set(user_train).union(set(user_test))
business = set(business_train).union(set(business_test))

userD = {}
userD_r = {}
busD = {}
busD_r = {}
for i, u in enumerate(user):
    userD[u] = i
for i, u in enumerate(user):
    userD_r[i] = u
for j, b in enumerate(business):
    busD[b] = j
for j, b in enumerate(business):
    busD_r[j] = b
user_length = len(user)
business_length = len(business)

if case_id == 1:
    # model based recommendation
    train = train.map(lambda x : Rating(userD[x[0][0]], busD[x[0][1]], x[1]))
    test = test_truth.map(lambda x : (userD[x[0][0]], busD[x[0][1]]))
    rank = 3
    numIterations = 10
    M = ALS.train(train, rank, numIterations)
    prediction = M.predictAll(test)
    def clean_predict(x):
        if x[2] > 5:
            clean_rate = 5
        elif x[2] <1 :
            clean_rate = 1
        else:
            clean_rate = x[2]
        return ((x[0],x[1]), clean_rate)
    predict = prediction.map(clean_predict)
    # average_rate = predict.map(lambda x : x[1]).mean()
    nopredict = test_key.map(lambda x : ((userD[x[0][0]], busD[x[0][1]]),None)).subtractByKey(predict).map(lambda x : (x[0],3))
    predict_dic = predict.collectAsMap() 
    nopredict_dic = nopredict.collectAsMap()
    
    with open(output_path, "w") as openfile:
        openfile.write("user_id, business_id, prediction \n")
        for x in predict_dic:
            l = ",".join([str(userD_r[x[0]]), str(busD_r[x[1]]), str(predict_dic[x])])
            openfile.write("%s\n" % l)
        for x in nopredict_dic:
            l = ",".join([str(userD_r[x[0]]), str(busD_r[x[1]]), str(nopredict_dic[x])])
            openfile.write("%s\n" % l)

elif case_id == 2:
    # user based recommendation
    train = train.map(lambda x : ((userD[x[0][0]], busD[x[0][1]]), x[1]))
    # ((user, business),rate)
    test = test_truth.map(lambda x : (userD[x[0][0]], busD[x[0][1]]))
    # (user, business)
    user_test_D = {}
    for u in user_test:
        user_test_D[userD[u]] = u
    # collect the test set user ids
    train_dic = train.collectAsMap()
    user_avg = train.map(lambda x : (x[0][0], [x[1]])).reduceByKey(lambda a,b : a+b).map(lambda x: (x[0], sum(x[1])/len(x[1])))
    train_user_avg = user_avg.collectAsMap()
    user_collect = train.map(lambda x : (x[0][1],[(x[0][0], x[1])])).reduceByKey(lambda a,b : a+b)
    # business [(user, rate),(user,rate)...], find the users who rate the same business

    # combinde two users with their common ratings to calculate the correlation
    def user_combine(x):
        ## find ((user1,user2),[(rate1,rate2)]), user1<user2
        for ur in it.combinations(x[1],2):
            u1, u2 = min(ur[0], ur[1]), max(ur[0], ur[1])
            yield ((u1[0],u2[0]), [(u1[1], u2[1])])
    user_combine = user_collect.flatMap(user_combine).reduceByKey(lambda a,b: a+b)

    # calculate the pearson correlation
    def pearson_cor(combine):
        rate1 = [x[0] for x in combine[1]]
        rate2 = [x[1] for x in combine[1]]
        avg1 = sum(rate1)/len(rate1)
        avg2 = sum(rate2)/len(rate2)
        numerater = sum([(rate1[i]-avg1)*(rate2[i] - avg2) for i in range(len(rate1))])
        de1 = sum([(r - avg1)**2 for r in rate1])
        de2 = sum([(r - avg2)**2 for r in rate2])
        if de1*de2 == 0:
            return 0
        else:
            return numerater/((de1*de2)**0.5)

    ## find all the correlation pairs with cor>=0.5
    user_correlation = user_combine.map(lambda x :(x[0], pearson_cor(x))).filter(lambda x : x[1] >= 0.6)

    ## find all the correlations related with one user which is in test set
    user_cor_matrix = user_correlation.flatMap(lambda a:[(a[0][0],[(a[0][1],a[1])]), (a[0][1],[(a[0][0],a[1])])]).filter(lambda x : x[0] in user_test_D).reduceByKey(lambda a,b: a+b)

    ## use the top 5 largest correlation
    def filter_simi(x):
        l = x[1]
        try:
            ll = sorted(l, key = lambda x:x[1], reverse = True)[:5]
            return (x[0],ll)
        except:
            return (x[0],l)

    user_cor_filter = user_cor_matrix.map(lambda x : filter_simi(x))
    cor_dic = user_cor_filter.collectAsMap()

    # user 5 top correlation to predict the rating in test set
    train_avg = train.map(lambda x : x[1]).mean()
    def predict_test(x):
        try:
            simi_users = cor_dic[x[0]]   
            average = train_user_avg[x[0]]
            num = 0
            dem = 0
            for e in simi_users:
                pair = (e[0], x[1])
                try:
                    rate = train_dic[pair] - train_user_avg[e[0]]
                    num += rate*e[1]
                    dem += abs(e[1])
                except:
                    average = average
            if dem == 0:
                return( x, average)
            else:
                return(x, num/dem + average)
        except:
            return (x,train_avg)

    def clean_predict_user(x):
        if x[1] > 5:
            clean_rate = 5
        elif x[1] <1 :
            clean_rate = 1
        else:
            clean_rate = x[1]
        return (x[0], clean_rate)

    prediction = test.map(predict_test).map(clean_predict_user)
    # truth_rating = test_truth.map(lambda x :((userD[x[0][0]], busD[x[0][1]]), x[1]))
    # cal_mse = truth_rating.join(prediction)
    # iff = cal_mse.map(lambda x: abs(x[1][0] - x[1][1])).map(lambda x : x**2).mean()
    # print("RMSE is: ", diff**0.5)

    prediction_dic = prediction.collectAsMap() 
    with open(output_path, "w") as openfile:
        openfile.write("user_id, business_id, prediction \n")
        for x in prediction_dic:
            l = ",".join([str(userD_r[x[0]]), str(busD_r[x[1]]), str(prediction_dic[x])])
            openfile.write("%s\n" % l)

elif case_id == 3:
    # item based _recommendation
    matrix_rdd = train.map(lambda x : (busD[x[0][1]],[userD[x[0][0]]])).reduceByKey(lambda a,b : a+b)
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
    result_jac = candidates_jac.flatMap(find_cand).map(jaccard).filter(lambda x : x[2] >= 0.5).map(lambda x: ((min(x[0], x[1]), max(x[0], x[1])), x[2])).distinct()
    trainint = train.map(lambda x : ((userD[x[0][0]], busD[x[0][1]]), x[1]))
    train_avg = trainint.map(lambda x: x[1]).mean()
    testint = test_truth.map(lambda x : ((userD[x[0][0]], busD[x[0][1]]), x[1]))
    train_dic = trainint.collectAsMap()
    business_avg = trainint.map(lambda x : (x[0][1], [x[1]])).reduceByKey(lambda a,b : a+b).map(lambda x: (x[0], sum(x[1])/len(x[1])))
    train_business_avg = business_avg.collectAsMap()
    simi_collect = result_jac.flatMap(lambda a:[(a[0][0],[(a[0][1],a[1])]), (a[0][1],[(a[0][0],a[1])])]).reduceByKey(lambda a,b: a+b)

    def filter_simi(x):
        l = x[1]
        try:
            ll = sorted(l, key = lambda x:x[1], reverse = True)[:5]
            return (x[0],ll)
        except:
            return (x[0],l)

    simi_filter = simi_collect.map(lambda x : filter_simi(x))
    simi_dic = simi_filter.collectAsMap()
    def predict_test(x):
        try:
            average = train_business_avg[x[1]]
            try:
                simi_business = simi_dic[x[1]]   
                num = 0
                dem = 0
                for e in simi_business:
                    pair = (x[0],e[0])
                    if pair in train_dic:
                        rate = train_dic[pair] - train_business_avg[e[0]]
                        num += rate*e[1]
                        dem += abs(e[1])
                if dem == 0:
                    return (x, average)
                else:
                    return (x, num/dem + average)
            except:
                return (x, average)
        except:
            return (x,train_avg)

    prediction = testint.map(lambda x :predict_test(x[0]))
    prediction_dic = prediction.collectAsMap() 
    with open(output_path, "w") as openfile:
        openfile.write("user_id, business_id, prediction \n")
        for x in prediction_dic:
            l = ",".join([str(userD_r[x[0]]), str(busD_r[x[1]]), str(prediction_dic[x])])
            openfile.write("%s\n" % l)
    # truth_rating = test_truth.map(lambda x :((userD[x[0][0]], busD[x[0][1]]), x[1]))
    # cal_mse = truth_rating.join(prediction)
    # diff = cal_mse.map(lambda x: abs(x[1][0] - x[1][1])).map(lambda x : x**2).mean()
    # print(" RMSE is: ", diff**0.5)
else:
    print("Please choose from 1,2 or 3 for case_id")

sc.stop()
print("running time is: ", time.time()-start_time)

