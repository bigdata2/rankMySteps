import sys
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
from pyspark import SparkConf
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import named_tuple_factory
import datetime

def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']


if __name__ == "__main__":

    conf = SparkConf().setAppName("rankmysteps")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 19)

    zkQuorum = "52.88.57.240:2181" 
    topic = "steps_data_part4"
    brokers = "52.88.57.240:9092"
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    lines = kvs.map(lambda x: x[1])
    
    def updateFunc(new_value, last_state):
        if last_state is None:
            last_state = new_value
            return last_state
        else:
            if new_value:
                [(steps_new,dt_new)] = new_value
                [(steps_old,dt_old)] = last_state
                total_steps = steps_new + steps_old
                return [(total_steps, dt_new)]
            else:
                return last_state

    def print_users(rdd):
	for (uuid, (steps,time)) in rdd.collect():
                print("user: %s steps taken: %s time: %s"% (uuid, steps, time))

    def write_into_cassandra(record):
        # connect to cassandra
        cluster = Cluster(['ec2-52-88-171-137.us-west-2.us-west-2.compute.amazonaws.com'])
        session = cluster.connect("rank_steps") 
        write_query_walker_steps = session.prepare("INSERT INTO walkers_steps"\
                                               "(user, num_steps, "\
                                               "arrival_time) VALUES (?,?,?)")
        write_query_walker_steps.consistency_level = ConsistencyLevel.QUORUM
        read_query = session.prepare("SELECT user, num_steps FROM "\
                                     "walkers_steps WHERE"\
                                     " user = ? and arrival_time = ?")
        read_query.consistency_level = ConsistencyLevel.QUORUM
	for (uuid, (steps,time)) in record:
                dt = datetime.datetime.strptime(time, '%Y-%m-%d %H:%M:%S')
                rows = session.execute(read_query,(uuid,dt.strftime('%Y-%m-%d')))
                if rows:
                    userid = rows[0].user
                    old_steps = int(rows[0].num_steps)
                    total_steps = old_steps + steps
                    session.execute(write_query_walker_steps, (uuid, total_steps,\
                                    dt.strftime('%Y-%m-%d')))
                else:
                    session.execute(write_query_walker_steps, (uuid, steps,\
                                    dt.strftime('%Y-%m-%d')))
        
    running_count = lines.map(lambda l: Row(uuid=json.loads(l)["uuid"],
                              steps=json.loads(l)["steps"], 
                              timestamp=json.loads(l)["timestamp"]))\
                         .map(lambda u: (u[2],(u[0],u[1])))\
                         .reduceByKey(lambda l1, l2: (l1[0]+l2[0], l2[1]))
                         #.updateStateByKey(updateFunc)

    #running_count.foreachRDD(print_users) #for debugging 
    running_count.foreachRDD(lambda rdd: \
                             rdd.foreachPartition(lambda record: \
                                                  write_into_cassandra(record)))

    ssc.start()
    ssc.awaitTermination()
