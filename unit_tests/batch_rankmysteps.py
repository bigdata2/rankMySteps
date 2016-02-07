from __future__ import print_function

import sys
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext, Row, Window
from pyspark.sql.functions import rowNumber, col
from pyspark.sql.types import *
from operator import add
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from datetime import datetime

if __name__ == "__main__":

    sc = SparkContext(appName="rankmysteps_batch")
    sqlCtx = SQLContext(sc)
    lines = sc.textFile("/home/ubuntu/r1/src/RealTime/python/test1.dat")
    users = lines.map(lambda l: Row(uuid=json.loads(l)["uuid"],steps=json.loads(l)["steps"]))
    #aggregated_steps = users.map(lambda u: (u[1],u[0])).reduceByKey(add).collect()
    aggregated_steps = users.map(lambda u: (u[1],u[0])).reduceByKey(add)
    df = sqlCtx.createDataFrame(aggregated_steps,['uuid', 'steps'])
    window = Window.partitionBy("uuid").orderBy("steps").rowsBetween(-1, 1)
    sorted_df = df.sort(df.steps.desc())
    df.select("uuid", "steps",
         rowNumber().over(Window.partitionBy("steps").orderBy("steps")).alias("rowNum")).show()

    sorted_df.show() 
    df.show()
    
    sc.stop()
