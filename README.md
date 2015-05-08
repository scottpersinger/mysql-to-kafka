# Mysql-kafka-replication

Demonstration of using Mysql replication protocol to stream Mysql changes into Kafka queues, then
read from those queues and process Mysql changes in a Node.js script.

## Installation

    pip install -r requirements.txt
    npm install

Oh, and go setup [Kafka](http://kafka.apache.org/) and get it running on `localhost:9092`.

## Running

Set `RDS_URL` in your environment, or add to a local `.env` file and run with `foreman`.

First, run the Mysql replication listener:

    python mysql-replicate-to-kafka.py

and then run the Node.js app:

    node consumer.js

Now go make some changes to your database and see them appear in the console of the node app.


