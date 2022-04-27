# MSTSpatial

This repo contains our code base for our SSDS course project. In this we have developed a novel indexing technique for answering spatio-temporal range queries on spatio-temporal data.

It is based on PySpark. Below are the instructions to get the code up and running.

1. Clone this repo .. ```git clone https://github.com/varadkulkarni11/MSTSpatial.git```
2. Download a .osm dataset file from https://download.geofabrik.de/asia/india.html
3. Preprocess using read_xml.py by running python read_xml.py. Set the 'dataset_file' variable to your input osm file and 'fname' to your output file path 
4. Upload the generated file in step 3 to hadoop in a directory named /tmp: hdfs dfs -copyFromLocal big.txt /tmp
5. Maintain Spatio-Temporal Range queries of the form (t,x1,x2,y1,y2) in 'queries.txt' in the same directory as your code
6. Run the following command:
     spark-submit --master yarn --deploy-mode cluster --driver-memory 2G --num-executors 4 --executor-cores 4 --executor-memory 16G run.py
7. Once done, your indexes will be created and stored in hadoop
8. In execute_queries.py, set the variables 'mos' and 'naive' to the hadoop directories of the indexes generated in the previous step
9. Run the command:
      spark-submit --master yarn --deploy-mode cluster --driver-memory 2G --num-executors 4 --executor-cores 4 --executor-memory 16G execute_queries.py
10. Your outputs for naive as well as effiecient approaches will be saved in 'naive_answers.txt' and 'mos_answers.txt'
11. Run diff naive_answers.txt mos_answers.txt to check the correctness




WIP: make it more user friendly, current state is raw
