import sys
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from datetime import datetime

if __name__ == "__main__":
    sc = SparkContext(appName="rankmysteps")
    ssc = StreamingContext(sc, 1)

    zkQuorum = "52.88.57.240:2181" 
    topic = "steps_data_part4"
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    lines = kvs.map(lambda x: x[1])
    


    def write_into_cassandra(record) :
        from cassandra.cluster import Cluster
        from cassandra import ConsistencyLevel

        # connect to cassandra
        cluster = Cluster(['ec2-52-35-237-159.us-west-2.compute.amazonaws.com'])
        session = cluster.connect("ranksteps") 
        prepared_write_query = session.prepare("INSERT INTO step_count (uuid, step_cnt) VALUES (?,?)") 
        for i in record:
            print (i)
            json_str = json.loads(i) 
            print (json_str)
            uuid = str(json_str["uuid"])  
            steps = str(json_str["steps"])  
            
            session.execute(prepared_write_query, (uuid, steps))

    def process(rdd):
        rdd.foreachPartition(lambda record: write_into_cassandra(record))
        

    lines.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()
