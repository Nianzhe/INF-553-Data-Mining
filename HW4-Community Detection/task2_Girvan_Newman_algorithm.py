from pyspark import SparkContext
import time 
import sys

input_path = sys.argv[1]
betweenness_path = sys.argv[2]
community_path = sys.argv[3]

start_time = time.time()
sc = SparkContext()
sc.setLogLevel("ERROR")

text = sc.textFile(input_path)
rdd = text.map(lambda x : x.split(" "))
edges = rdd.flatMap(lambda x: [(x[0], [x[1]]), (x[1], [x[0]])]).reduceByKey(lambda a,b: a+b)
edges_dic = edges.collectAsMap()

## 1. calculate betweenness using Girvan-Newman algorithm
def GN(n,edges_dic):
    root = n
    level_dic = {root:0}
    parent_dic = {}
    bfs_nodes = [root]
    ind = 0
    # 1st, find the level of each node
    while ind < len(bfs_nodes):
        parent = bfs_nodes[ind]
        kids = edges_dic[parent]
        for kid in kids:
            if kid not in bfs_nodes:
                bfs_nodes.append(kid)
                level_dic[kid] = level_dic[parent]+1
                parent_dic[kid] = [parent]
            elif level_dic[kid] == level_dic[parent] +1:
                parent_dic[kid] = parent_dic[kid] + [parent]
        ind +=1
        
    # 2nd, find the count of shortest path of each node
    level_dic_r = {}
    for key,value in level_dic.items():
        level_dic_r[value] = level_dic_r.get(value,[])
        level_dic_r[value].append(key)
    max_level = max(level_dic_r)
    count_dic = {root:1}
    level = 1
    while level <= max_level:
        current_nodes = level_dic_r[level]
        for n in current_nodes:
            if level == 1:
                count_dic[n] = 1
            else:
                current_parents = parent_dic[n]
                for p in current_parents:
                    count_dic[n] = count_dic.get(n,0) + count_dic[p]
        level +=1
        
    # 3rd, calculate the betweeness of each edge
    weight_dic = {}
    for x in level_dic:
        weight_dic[x] = 1
    for i in range(len(bfs_nodes)-1,-1,-1):
            node = bfs_nodes[i]
            if node in parent_dic:
                parents = parent_dic[node]
                parent_size = 0
                for parent in parents:
                    parent_size += count_dic[parent]
                for parent in parents:
                    add = float(weight_dic[node])/parent_size*count_dic[parent]
                    weight_dic[parent] += add
                    current_edge = (min(node,parent), max(node, parent))
                    yield (current_edge, add/2)


result = edges.flatMap(lambda x: GN(x[0],edges_dic)).reduceByKey(lambda a,b: a+b).collect()
result_sorted = sorted(result, key = lambda x: [-x[1], x[0][0]])

with open(betweenness_path,"w") as openfile:
    for x in result_sorted:
        s = str(x[0]) +"," +str(x[1])+"\n"
        openfile.write(s)
openfile.close()


## 2. Community Seperation

m = len(result)
nodes_list = edges.map(lambda x: x[0]).collect()
nodes_rdd = sc.parallelize(nodes_list)
degree_dic = edges.map(lambda x: (x[0], len(x[1]))).collectAsMap()
edge_list = rdd.map(lambda x:(min(x[0],x[1]), max(x[0],x[1]))).collect()

matrix_A = {}
for i in edge_list:
    matrix_A[i] = 1

# find current cluster
def dfs(current, n, connect_dic, dic):
    connect_dic[n] = True
    current.append(n)
    for i in dic[n]:
        if connect_dic[i] == False:
            current = dfs(current,i,connect_dic, dic)
    return current

# find all the clusters
def connect_nodes(dic):
    connect_dic = {n:False for n in dic}
    connect_list = []  
    for n in dic:
        if connect_dic[n] == False:
            current = []
            connect_list.append(dfs(current, n, connect_dic, dic))
    return connect_list

# calculate modularity
def compute_mod(connect_list):
    modularity =0 
    for x in connect_list:
        for i in x:
            for j in x:
                if i!= j:
                    pair = (min(i,j), max(i,j))
                    ad = (matrix_A.get(pair,0) - degree_dic[i]*degree_dic[j]/2/m)/2/m
                    modularity += ad
    return modularity


dic = edges.collectAsMap()
max_modularity = 0
i = 0
clusters = []
while i < m:
    filter_out = nodes_rdd.flatMap(lambda x: GN(x,dic)).reduceByKey(lambda a,b: a+b).takeOrdered(1, key = lambda x: -x[1])
    ll = filter_out[0][0]
    dic[ll[0]].remove(ll[1])
    dic[ll[1]].remove(ll[0])
    connected_list = connect_nodes(dic)
    modularity = compute_mod(connected_list)
    if modularity >= max_modularity:
        max_modularity = modularity
        clusters = connected_list
    elif len(connected_list) > m/3*2:
        break
    i = i+1


final_list = []
for x in clusters:
    final_list.append((sorted(x),len(x)))
final_list = sorted(final_list, key = lambda x: [x[1],x[0]]) 

with open(community_path,"w") as openfile:
    for x in final_list:
        s = "','".join(x[0])
        s = "'" + s + "'"
        openfile.write("%s\n" %s)
openfile.close()
sc.stop()
print("Duration: ", time.time()- start_time)







