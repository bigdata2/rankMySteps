import os
import sys
import boto3
import time
from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
import os

class Consumer(object):

    def __init__(self, addr):
        self.client = KafkaClient(addr)
        self.topic = "steps_data_part4"
        self.consumer_group = 's3_consumer' 
        self.consumer = SimpleConsumer(self.client, self.consumer_group, self.topic)

    def consume_message(self):
        while True:
            timestamp = time.strftime('%Y%m%d%H%M%S')
            temp_file_name = "%s_%s_%s.dat" %(self.topic, self.consumer_group, timestamp)
            temp_file = open("/home/ubuntu/rankMyStep/kafka/"+temp_file_name,"w")
            messages = self.consumer.get_messages(count=1000, block=False)
            for msg in messages:
                print msg.message.value + "\n"
                temp_file.write(msg.message.value + "\n")
            self.save_to_s3(temp_file_name)

    def save_to_s3(self, file_name):
        mybucket = "anurag-raw-data-store"
        aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
        aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')
        s3_client = boto3.client('s3')
        s3_client.upload_file("/home/ubuntu/rankMyStep/kafka/"+file_name, 
                              mybucket,"rankmysteps/"+file_name)
        os.remove("/home/ubuntu/rankMyStep/kafka/"+file_name)

if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    consumer = Consumer(ip_addr)
    consumer.consume_message()
