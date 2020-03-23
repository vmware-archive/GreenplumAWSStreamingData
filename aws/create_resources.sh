#!/usr/bin/env bash

echo "*************************************************************************************************"
echo "*                             Greenplum Streams Demo app AWS resources setup                    *"
echo "*                           ---------------------------------------------------                 *"
echo "*************************************************************************************************"

echo "Creating S3 bucket greenplum-streams-demo in region us-east-1 ............"
aws s3api  create-bucket --bucket greenplum-streams-demo --region us-east-1
echo "creating a folder data/raw_transactions/ under greenplum-streams-demo bucket .............."
aws s3api put-object --bucket greenplum-streams-demo --key data/raw_transactions/
echo "creating a folder lambda/jars/ under greenplum-streams-demo bucket .............."
aws s3api put-object --bucket greenplum-streams-demo --key lambda/jars/

echo "Copying lambda jars to s3 ............."
# TODO: may be force check if jars are available ?
aws s3 cp ../KinesisFirehoseLambdaSink/target/KinesisFirehoseLambdaSink-1.0.0-SNAPSHOT.jar s3://greenplum-streams-demo/lambda/jars/
aws s3 cp ../S3GreenplumLambdaSink/target/S3GreenplumLambdaSink-1.0.0-SNAPSHOT.jar s3://greenplum-streams-demo/lambda/jars/

echo "Creating Kinesis Stream 'ctrans' with 3 shards ......."
aws kinesis create-stream --stream-name credtrans_stream --shard-count 1
aws kinesis list-streams

echo "Creating role credtrans-lamda-ex role ....."
aws iam create-role --role-name credtrans-lamda-ex --assume-role-policy-document '{"Version": "2012-10-17","Statement": [{ "Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]}'

echo "attaching policies AWSLambdaBasicExecutionRole, AWSLambdaKinesisExecutionRole, AmazonKinesisFirehoseFullAccess,
AmazonS3FullAccess,  AmazonSQSFullAccess to role credtrans-lamda-ex ....."
aws iam attach-role-policy --role-name credtrans-lamda-ex --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
aws iam attach-role-policy --role-name credtrans-lamda-ex --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaKinesisExecutionRole
aws iam attach-role-policy --role-name credtrans-lamda-ex --policy-arn arn:aws:iam::aws:policy/AmazonKinesisFirehoseFullAccess
aws iam attach-role-policy --role-name credtrans-lamda-ex --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
aws iam attach-role-policy --role-name credtrans-lamda-ex --policy-arn arn:aws:iam::aws:policy/AmazonSQSFullAccess

ctrn_rolearn=`aws iam get-role --role-name credtrans-lamda-ex | sed -n 's/"Arn"://p' | sed 's/"//g' | sed 's/,//g'`
echo "role arn -> $ctrn_rolearn "

ctrn_stream_arn=`aws kinesis describe-stream --stream-name credtrans_stream | sed -n 's/"StreamARN"://p' | sed s'/"//g' | sed 's/,//g'`
echo "stream arn -> $ctrn_stream_arn"

echo "creating lambda function credtrans_ks_event_lambda ....."
aws lambda create-function \
        --function-name credtrans_ks_event_lambda \
        --runtime java8 \
        --role $ctrn_rolearn \
        --handler io.greenplum.demo.streaming.firehose.CreditTransactionsFirehoseHandler::handleRequest \
        --code S3Bucket=greenplum-streams-demo,S3Key=lambda/jars/KinesisFirehoseLambdaSink-1.0.0-SNAPSHOT.jar

echo "creating function event trigger ....."
aws lambda create-event-source-mapping \
        --event-source-arn $ctrn_stream_arn \
        --function-name credtrans_ks_event_lambda \
        --enabled \
        --batch-size 100 \
        --maximum-batching-window-in-seconds=1 \
        --parallelization-factor=1 \
        --starting-position=TRIM_HORIZON

# create role and policy for firehose delivery stream
echo "Creating credtrans-firehose-role and credtrans-firehose-policy ........."
aws iam create-role --role-name credtrans-firehose-role --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Sid":"","Effect":"Allow","Principal":{"Service":"firehose.amazonaws.com"},"Action":"sts:AssumeRole","Condition":{"StringEquals":{"sts:ExternalId":"541124183456"}}}]}'


aws iam create-policy --policy-name credtrans-firehose-policy --policy-document file://credtrans-firehose-policy.json

crtn_firehose_arn=`aws iam list-policies | grep credtrans-firehose-policy | sed -n 's/"Arn"://p' | sed s'/"//g' | sed s'/,//g'`

aws iam attach-role-policy --role-name credtrans-firehose-role --policy-arn arn:aws:iam::aws:policy/AmazonKinesisFirehoseFullAccess

--$crtn_firehose_arn
ctrn_fh_rolearn=`aws iam get-role --role-name credtrans-firehose-role | sed -n 's/"Arn"://p' | sed 's/"//g' | sed 's/,//g'`
echo "role arn -> $ctrn_fh_rolearn "


echo "Creating Kinesis Firehose credtrans-s3-firehose ..........."

fh_s3_config=''{\"RoleARN\":\"$ctrn_fh_rolearn\",\"BucketARN\":\"arn:aws:s3:::greenplum-streams-demo\",\"Prefix\":\"data/raw_transactions/\",\"BufferingHints\":{\"SizeInMBs\":2,\"IntervalInSeconds\":60},\"CloudWatchLoggingOptions\":{\"Enabled\":true}}''
aws firehose create-delivery-stream \
                --delivery-stream-name credtrans-s3-firehose \
                --delivery-stream-type DirectPut \
                --s3-destination-configuration '{"RoleARN": "arn:aws:iam::541124183456:role/credtrans-firehose-role","BucketARN": "arn:aws:s3:::greenplum-streams-demo","Prefix": "data/raw_transactions/","BufferingHints": {"SizeInMBs": 2,"IntervalInSeconds": 60},"CloudWatchLoggingOptions":{"Enabled": true, "LogGroupName": "/aws/kinesisfirehose/credtrans-test-ds", "LogStreamName": "S3Delivery"}}'
