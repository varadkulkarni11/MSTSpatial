#application_1650899165150_0021-big,29-large,38-bigmst,39-largemst
#polaris application_1650523863206_0118 -large mst
from pyspark import *
from pyspark.sql import *
import math
import xml.etree.ElementTree as ET
import time
dataset_file="/tmp/large.txt"
queries_file="queries.txt"
mos='Mos_Algo_Based_Index_3'
naive ='Naive_Spatio_Temporal_3'

spatial=[]
numPartitions=8


# dataset_file="/user/varadk/big.txt"
# queries_file="/home/varadk/ssds/queries.txt"
# mos='/user/varadk/Mos_Algo_Based_Index_2'
# naive ='/user/varadk/Naive_Spatio_Temporal_2'
# spatial=[]
# numPartitions=16

def process(x):
	ll=[]
	ll.append([x[0],x[1],x[2],spatial])
	return ll

def srch_min_ind(x,key1,key2):
	for i in range(len(x)):
		if(x[i][0]>=key1 and x[i][0]<=key2):
			return i
	return -1


def merge_sort_tree(y,queries):
	n=len(y)
	tree=[[] for i in range(4*n+2)]
	def build_tree(idx,l,r):
	    if l==r:
	        tree[idx].append(y[l])
	    else:
	        mid=(l+r)//2
	        build_tree(2*idx+1,l,mid)
	        build_tree(2*idx+2,mid+1,r)
	        tree[idx]=merge(tree[2*idx+1],tree[2*idx+2])

	def merge(left,right):
	    myList=[]
	    i=0
	    j=0
	    while i < len(left) and j < len(right):
	        if left[i] < right[j]:

	            myList.append(left[i])

	            i += 1
	        else:
	            myList.append(right[j])
	            j += 1



	    while i < len(left):
	        myList.append(left[i])
	        i += 1


	    while j < len(right):
	        myList.append(right[j])
	        j += 1

	    return myList


	def f_gr(x,k):
		
		anss=len(x)
		l=0
		r=len(x)-1
		while(l<=r):
			mid=(l+r)//2
			if x[mid]>=k:
				anss=mid
				
				r=mid-1
			else:
				l=mid+1

		return len(x)-anss

	def f_ls(x,k):
		
		anss=0
		l=0
		r=len(x)-1
		while(l<=r):
			mid=(l+r)//2
			if x[mid]<=k:
				anss=mid+1
				l=mid+1
			else:
				r=mid-1

		
		return anss

	def query_greater(idx,st,en,l,r,k):
		if l>en or st>en or r<st:
			return 0
		if st>=l and en<=r:
			return f_gr(tree[idx],k)

		mid=(st+en)//2
		q1=query_greater(2*idx+1,st,mid,l,r,k)
		q2=query_greater(2*idx+2,mid+1,en,l,r,k)
		return q1+q2

	def query_lesser(idx,st,en,l,r,k):
		if l>en or st>en or r<st:
			return 0
		if st>=l and en<=r:
			return f_ls(tree[idx],k)

		mid=(st+en)//2
		q1=query_lesser(2*idx+1,st,mid,l,r,k)
		q2=query_lesser(2*idx+2,mid+1,en,l,r,k)
		return q1+q2

	build_tree(0,0,n-1)
	anss=[]
	for query in queries:
		l,r,y1,y2,x1,x2=query
		xx=query_lesser(0,0,n-1,l,r,y2)
		yy=query_greater(0,0,n-1,l,r,y1)
		diff=r-l+1

		anss.append((xx+yy-diff))
	return anss

def without():
	ans=[]
	kk=0
	for query in queries:
		ct=0
		l,r,y1,y2,x1,x2=query
		if(l==-1 and r==-1):
			continue
		if l==-1:
			continue
		if r==-1:
			continue
		for i in range(l,r+1):
			if y[i]>=y1 and y[i]<=y2:
				ct+=1
		ans.append(((x1,x2,y1,y2),ct))
		kk+=1
	return ans

def pre_process_queries(y,queries):
	##TO-DO : Implement mergeSortTree
	anss=merge_sort_tree(y,queries)
	ans=[]
	kk=0
	for query in queries:
		ct=anss[kk]
		l,r,y1,y2,x1,x2=query
		ans.append(((x1,x2,y1,y2),ct))
		kk+=1
		
	# without()
	return ans

def srch_max_ind(x,key1,key2):
	mx=-1
	for i in range(len(x)):
		if(x[i][0]>=key1 and x[i][0]<=key2):
			mx=i
	return mx


def return_lrs(x,points):
	l=0
	r=0
	lrs=[]
	for point in points:
		lrs.append((srch_min_ind(x,point[0],point[1]),srch_max_ind(x,point[0],point[1]),point[2],point[3],point[0],point[1]))
	return lrs

def get_lrs(x):
	lrs=return_lrs(x[0][1],x[0][3])
	return [x[0][0],x[0][1],x[0][2],lrs]

def foo(x):
	yy=[]
	xx=[]
	for i in range(len(x[1])):
		yy.append(x[1][i][1])
		xx.append([x[1][i][0],i])
	return [x[0],xx,yy]

def conv_to_dict(x):
	answers=dict()
	for lol in x[1]:
		answers[(lol[0][0],lol[0][1],lol[0][2],lol[0][3])]=lol[1]
	return answers

def our_own_partition():
	print("Our Own: Mos Algo")
	input_rdd=sc.textFile(dataset_file).map(lambda x:p(x))
	rdd=input_rdd.map(lambda x:(x[1],x))
	rdd=rdd.sortByKey().map(lambda x:(x[1][0],[x[1][1],x[1][2]]))
	rdd=rdd.groupByKey().mapValues(list)
	rdd=rdd.partitionBy(numPartitions,partition_by_time)
	rdd=rdd.map(lambda x: foo(x))
	rdd=rdd.map(lambda x:process(x))
	rdd=rdd.map(lambda x:get_lrs(x))
	rdd=rdd.map(lambda x:[x[0],pre_process_queries(x[2],x[3])]).filter(lambda x:(len(x[1])>0)).map(lambda x:(x[0],conv_to_dict(x)))
	# lst = rdd.collect()
	# print(lst)
	rdd.toDF().write.save(mos)
	


def base_line_partition():
	print("Base line: Naive")
	input_rdd=sc.textFile(dataset_file).map(lambda x:p(x))
	input_rdd.toDF().write.save(naive)


def partition_by_time(time):
	return int(time)%numPartitions


def p(x):
	return [float(a) for a in x.split()]



def init_spark():
        print('Initializing Spark')
        rdd1=sc.parallelize([1,2,3,4])
        init=rdd1.count()
        print('INIT DONE!')

def profile_naive():
        naive_start_time=time.time()
        print('STARTING NAIVE INDEX BUILDING')
        base_line_partition()
        naive_end_time=time.time()
        naive_time=naive_end_time - naive_start_time
        print('ENDED NAIVE INDEX BUILD, TIME TAKEN: '+str(naive_time) +' sec')

def profile_mos():
        mos_start_time=time.time()
        print('STARTING MOS INDEX BUILDING')
        our_own_partition()
        mos_end_time=time.time()
        mos_time=mos_end_time - mos_start_time
        print('ENDED MOS INDEX BUILD, TIME TAKEN: '+str(mos_time)+' sec')

if __name__=='__main__':

	spark = SparkSession.builder.appName("VkTest").getOrCreate()
	sc = spark.sparkContext
	del spatial[:]
	#spatial.clear()
	spatial_set=set()

	with open(queries_file) as infile:
		for line in infile:
			query=[float(f) for f in line.split()]
			spatial_set.add((query[1],query[2],query[3],query[4]))
	
	for val in spatial_set:
		spatial.append(val)

	init_spark()
	profile_naive()
	profile_mos()
	
	
