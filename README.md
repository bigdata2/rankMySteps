# RankMySteps
### An app for counting and ranking top users based on number of steps taken

#### Introduction

This app was inspired by fitbit and jawbone like wearables that count the number of steps taken by the users and periodically send the total back to the application.
I wanted to create a list of top-n users for each day so that the users can see how far behind or ahead they are compared to other users of the app and challenge themselves.

See a presentation of this app [here](http://rankmysteps.xyz)

#### Infrastructure

The data pipeline for this app has been setup on AWS. It uses eight m4.xlarge instances that house the following open source tools:
- Apache Kafka
- Amazon S3
- Spark Streaming
- Cassandra
- Flask web app

Four of the eight instances are dedicated to Kafka brokers and the other four contain Spark Streaming and Cassandra both. One of the instances is also used to house flask server.

#### Data Pipeline Description

Six producer scripts produce JSON messages that contain a unique uuid, timestamp when message was generated and number of steps taken by the user. The uuid and number of steps are generated using a pseudorandom generator.

![alt text](https://github.com/bigdata2/rankMySteps/blob/master/images/data.png "JSON messages")

The data from producer scripts flow into four partition single topic kafka queue. The output of Kafka connects to S3 which is the source of truth in this pipeline and in future will be used for batch processing. Spark Streaming gets data from Kafka through a direct connection. Spark streaming aggregates data into the microbatch and assign the latest timestamp to the aggregated messages. The data for a uuid is then read from Cassandra and written back into it. A materialized view is created from the data written in Cassandra to sort the data stored.

![alt text](https://github.com/bigdata2/rankMySteps/blob/master/images/data.png "Data Pipeline")

The schema and materialized view in Cassandra are explained below:

![alt text](https://github.com/bigdata2/rankMySteps/blob/master/images/data.png "Cassandra Schema")
https://github.com/bigdata2/rankMySteps/blob/master/images/schema.png
