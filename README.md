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

Six producer scripts produce JSON messages that contain a unique uuid, timestamp when message was generated and number of steps taken by the user. The uuid and number of steps (between 1-10) are generated using a pseudorandom generator. The pipeline has been tested with over million users but for the purpose of demo, the number of users have been limited to seventy two thousand.

![alt text](https://github.com/bigdata2/rankMySteps/blob/master/images/data.png "JSON messages")

The data from producer scripts flow into four partition single topic kafka queue. The output of Kafka connects to S3 which is the source of truth in this pipeline and in future will be used for batch processing. Spark Streaming gets data from Kafka through a direct connection. Spark streaming aggregates data into the microbatch and assign the latest timestamp to the aggregated messages. The data for a uuid is then read from Cassandra and written back into it. A materialized view is created from the data written in Cassandra to sort the data stored.

![alt text](https://github.com/bigdata2/rankMySteps/blob/master/images/pipeline.png "Data Pipeline")

The schema and materialized view in Cassandra are explained below:

![alt text](https://github.com/bigdata2/rankMySteps/blob/master/images/schema.png "Cassandra Schema")

#### Challanges with Data Denormalization and Materialized Views (MV) in Cassandra 3.0
Initially I developed this app with two tables. The first table was used as a key value store and the second table was used for clustering based on date to sort on total number of steps taken by the users. I used Cassandra 2.2 for that solution. 

Since the Primary Key (PK) for the second table had the number of steps as a component, there were several entries for some users. For example, a user who is walking/running and is continuously trasmitting data every second will have several more entries than a user who trasmits data once every several hours.

This issue caused finding top-N walkers challanging. I initially took top hundred thousand entries from the second table and removed duplicate entries for a user in the application. However, even with duplicates removed, in the worst case, a very active user can potentially take up most of the top hundred thousand entries. To overcome this problem I tried to delete the entries from the second table based on the values read from the first table. However, I found that when running the pipeline with approximately 5000 messages per second, some records were not getting deleted from the second table. That caused the first and second table in inconsistent state.  

Upon researching this problem I stumbled upon batch updates and materialized views in Cassandra. The latter is only available in Cassandra 3.0 and above. I chose to use materialized view for this project and have summarized its salient points below:

- Eliminate the need of data denormalization by developers -- No need to create multiple tables for different queries.

- Can be queried as any Cassandra table.

- Persistent view — NOT an SQL view.

- Automatic propagation of updates from the base table to MV ensuring eventual consistency.
