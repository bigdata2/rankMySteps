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
from operator import add

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
        prepared_write_query = session.prepare("INSERT INTO step_count2 (uuid, step_cnt) VALUES (?,?)") 
        for i in record:
            print (i)
            json_str = json.loads(i) 
            print (json_str)
            uuid = str(json_str["uuid"])  
            steps = str(json_str["steps"])  
            
            session.execute(prepared_write_query, (uuid, steps))

    def add_steps(v1,v2):
        total_steps = v1[0] + v2[0]
        date1 = (datetime.datetime.strptime(v1[1], '%Y-%m-%d %H:%M:%S') -    
                 datetime.datetime(1970,1,1,0,0,0)).total_seconds()
        date2 = (datetime.datetime.strptime(v2[1], '%Y-%m-%d %H:%M:%S') -    
                 datetime.datetime(1970,1,1,0,0,0)).total_seconds()
        latest_date = v1[1] if (date1 > date2) else v2[1]
        return (total_steps, latest_date)

    def process(rdd):
        users = rdd.map(lambda l: Row(uuid=json.loads(l)["uuid"],
                          steps=json.loads(l)["steps"], timestamp=json.loads(l)["timestamp"]))
        aggregated_steps = users.map(lambda u: (u[2],(u[0],u[1]))).reduceByKey(add_steps).collect()
        for (uuid, (steps, date)) in aggregated_steps:
             print("%s: %s %s"% (uuid, steps, date))
        
    lines.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()
