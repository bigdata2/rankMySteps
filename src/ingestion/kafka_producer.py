import random
import sys
import six
import json
import pytz
from pytz import timezone
from datetime import datetime
from kafka.client import KafkaClient
from kafka.producer import KeyedProducer

class Producer(object):

    def __init__(self, addr):
        self.client = KafkaClient(addr)
        self.producer = KeyedProducer(self.client,async=True,\
                                      batch_send_every_n=500,batch_send=False)
        self.min_steps = 1
        self.max_steps = 3
        self.max_users_each_thread = 12000

    def produce_msgs(self, source_symbol):
        msg_cnt = 0
        while True:
            start_uuid = (int(source_symbol) - 1) * self.max_users_each_thread
            stop_uuid =  (int(source_symbol) * self.max_users_each_thread) - 1
            uuid = random.sample(range(start_uuid,stop_uuid), 9)
            for uid in uuid:
                timestamp = datetime.now(timezone('US/Pacific')).\
                                        strftime('%Y-%m-%d %H:%M:%S')
                steps = random.randint(1,10)
                json_msg= {'source':source_symbol,'uuid':uid, 
                           'timestamp':timestamp, 'steps': steps}
                json_encoded = json.dumps(json_msg)
                self.producer.send_messages('steps_data_part4', source_symbol,\
                                             json_encoded)
                print json_encoded
                msg_cnt += 1
                #sleep(2)

if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    partition_key = str(args[2])
    prod = Producer(ip_addr)
    prod.produce_msgs(partition_key) 
