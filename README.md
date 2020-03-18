# GreenplumStreamingData
Demonstrates Greenplum Kinesis data flow and kafka data flow, and GPSS API

This Git project goal is to demonstrate Streaming data to Greenplum with different dataflow mechanisms. The supported dataflow aspects are;
1. Amazon Kinesis Streams
2. Kafka Streaming using Greenplum connector
3. Gpfdist using local file store
4. Greenplum Streaming server API.

As of today the implementation is done only for Amazon Kinesis. The project contain two parts;
1. Feed producer that will synthsize Credit card card transactioins. This part can sink feed to Kinesis stream, Kafaka Topic, console, FIle store.
2. This is slpit into individual processors of feed. 

For Kinesis it is implmented as JAva 8 Lambda appliaction. That reacts to stream and inserts records to Greenplum.

This repo is being developed actively. So could be inconsistent at times.

The data that is being generated is a java port from Python of MADLibflow python, node.js demo that was showcased in gp summit last year. 

Next TODO:
1. Kafak topic sink -- target MAR 11, 2020
2. File store sink -- target MAR 11, 2020
3. Grafana dashboard to display trasactions as the feed comes in. -- Target MAR 13, 2020
4. GPSS API Sink -- Target MAR 16, 2020





