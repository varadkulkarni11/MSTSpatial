from pyspark import *
from pyspark.sql import *
import requests
import subprocess
import re
import time
tme=-1
mos='/user/varad.kulkarni/Mos_Algo_Based_Index_1'
naive ='/user/varad.kulkarni/Naive_Spatio_Temporal_1'
queries_file="queries.txt"
naive_answers='naive_answers.txt'
mos_answers='mos_answers.txt'
current_query=(-1,-1,-1,-1,-1)
numPartitions=8
prefix='-3331b160-f55e-4ebb-994f-82ad91a0f142-c000.snappy.parquet'

def get_sorted_file_list(files_dir):
	fk=dict()
	'''
	key =  files_dir
	files = subprocess.run(['ls', key+'/'], stdout=subprocess.PIPE)
	files = files.stdout.decode().split()[0:numPartitions+1]	
	files = [file for file in files if '.parquet' in file]
	for file in files:
		num=re.findall(r'part-(\d+)-.+', file)
		files_with_partition_id[int(num[0])]='/'+str(file)

	# print('Returning files: '+str(files_with_partition_id))
	'''
	fk[0]='/part-00000'+prefix
	fk[1]='/part-00001'+prefix
	fk[2]='/part-00002'+prefix
	fk[3]='/part-00003'+prefix
	fk[4]='/part-00004'+prefix
	fk[5]='/part-00005'+prefix
	fk[6]='/part-00006'+prefix
	fk[7]='/part-00007'+prefix
	return fk

def find_matches(x):
	if (x[0]==current_query[0] and x[1]>=current_query[1] and x[1]<=current_query[2] and x[2]>=current_query[3] and x[2]<=current_query[4]):
		return 1
	return 0 

def get_partition(time):
	return int(time)%numPartitions

def get_partition_files():
	files=get_file_list(mos)

def bar(x):
	if (int(x[0]==tme)):
		return x[1]
	return []

def execute_queries_efficiently():
	total_file_read_time=0
	partition_files=get_sorted_file_list(mos)
	file=open(mos_answers, "w")
	global current_query
	global tme

	with open(queries_file) as infile:
		for line in infile:
			query=[float(f) for f in line.split()]
			current_query=(query[0],query[1],query[2],query[3],query[4])
			partition=get_partition(current_query[0])
			tme=current_query[0]
			fn = mos+partition_files[partition]
			#file_read_start_time=time.time()
			load_partition=spark.read.load(fn).rdd
			#dummy=load_partition.count()
			#file_read_end_time=time.time()
			#file_read_time=file_read_end_time-file_read_start_time
			#total_file_read_time+=file_read_time
			load_partition=load_partition.map(lambda x:bar(x)).filter(lambda x:len(x)>0)
			boxes_map=load_partition.collect()[0]
			file.write(str(boxes_map[(query[1],query[2],query[3],query[4])])+'\n')
	print('ALL DONE! TOTAL TIME SPENT IN FILE READ= '+str(total_file_read_time))
	file.close()



def execute_queries_naively():
	file=open(naive_answers, "w")
	global current_query
	load_all_partitions=spark.read.load(naive).rdd.persist()
	with open(queries_file) as infile:
		for line in infile:
			query=[float(f) for f in line.split()]
			current_query=(query[0],query[1],query[2],query[3],query[4])
			fin_rdd=load_all_partitions.map(lambda x:find_matches(x))
			file.write(str(fin_rdd.sum())+'\n')

	print('ALL DONE!')
	file.close()
			

def profile_naive():
	naive_start_time=time.time()
	print('STARTING NAIVE EXECUTION')
	execute_queries_naively()
	naive_end_time=time.time()
	naive_time=naive_end_time - naive_start_time
	print('ENDED NAIVE EXECUTION, TIME TAKEN: '+str(naive_time) +' sec')

def profile_mos():
	mos_start_time=time.time()
	print('STARTING MOS EXECUTION')
	execute_queries_efficiently()
	mos_end_time=time.time()
	mos_time=mos_end_time - mos_start_time
	print('ENDED MOS EXECUTION, TIME TAKEN: '+str(mos_time)+' sec')

def init_spark():
	print('Initializing Spark')
	rdd=spark.read.load(naive).rdd
	rdd1=sc.parallelize([1,2,3,4])
	init=rdd.count()
	init=rdd1.count()
	print('INIT DONE!')

if __name__ == '__main__':
	spark = SparkSession.builder.appName("VkTest").getOrCreate()
	sc = spark.sparkContext
	init_spark()

	# profile_naive()
	profile_mos()	
	
	


	

