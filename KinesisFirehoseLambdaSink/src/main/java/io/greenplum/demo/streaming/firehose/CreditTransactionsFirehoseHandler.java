package io.greenplum.demo.streaming.firehose;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesisfirehose.model.*;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.greenplum.demo.streaming.model.Transaction;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * @author sridhar paladugu
 * @version 1.0
 *
 * The class defines event handler to sink data to S3 firehore from a Kenisis Event.
 *
 */
public class CreditTransactionsFirehoseHandler implements RequestHandler<KinesisEvent , Void> {

    private String region;
    private String scoringQueueUrl;
    private String deliveryStreamName ;
    private AmazonKinesisFirehose amazonKinesisFirehose;
    private AmazonSQS amazonSQS;

    public void init(Context context) {
        context.getLogger().log("Initializing Kinesis firehose .......");
        region = System.getenv("AWS_REGION");
        deliveryStreamName = System.getenv("DELIVERY_STREAM_NAME");
        AmazonKinesisFirehoseClientBuilder amazonKinesisFirehoseClientBuilder = AmazonKinesisFirehoseClientBuilder.standard();
         ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setConnectionTimeout(60);
        amazonKinesisFirehoseClientBuilder.setClientConfiguration(clientConfiguration);
        amazonKinesisFirehoseClientBuilder.setRegion(region);
        amazonKinesisFirehose = amazonKinesisFirehoseClientBuilder.build();
        context.getLogger().log("Finished Initializing Kinesis firehose!");
        amazonSQS = AmazonSQSClientBuilder.defaultClient();
    }


    @Override
    public Void handleRequest(KinesisEvent kinesisEvent, Context context) {
        if(amazonKinesisFirehose == null) {
            init(context);
        }
        int countOfRecords = kinesisEvent.getRecords().size();
        context.getLogger().log("Received "+ countOfRecords +" transactions in the current batch.");
        Map<Integer, String> transactionJsonMap = new HashMap<Integer, String>();
        StringBuffer csv = new StringBuffer();;
        for (KinesisEvent.KinesisEventRecord rec : kinesisEvent.getRecords()) {
            Transaction transaction = Transaction.fromJsonAsBytes(rec.getKinesis().getData().array());
            if (transaction == null) {
                context.getLogger().log("Transaction received as null, so skipping record for partition -> " + rec.getKinesis().getPartitionKey());
            } else {
                // to s3 as csv file with all transactions in the batch
                csv.append(transaction.toCSV(","));
                //individual events to SQS for scoring
                transactionJsonMap.put(transaction.getTransactionId(), transaction.toJsonAsString());
            }

        }
        context.getLogger().log("Storing " + countOfRecords + " events to kinesis firehose ....");
        Record record = new Record().withData(ByteBuffer.wrap(csv.toString().getBytes()));
        PutRecordRequest putRecordRequest  = new PutRecordRequest()
                .withDeliveryStreamName(deliveryStreamName).withRecord(record);
        amazonKinesisFirehose.putRecord(putRecordRequest);
        context.getLogger().log("Delivered transaction events to S3!");

        scoringQueueUrl = System.getenv("SCORING_QUEUE_URL");
        context.getLogger().log("Dispatching " + countOfRecords + " events to "+ scoringQueueUrl + " queue....");
        transactionJsonMap.forEach((id, transaction) ->{
            SendMessageRequest sendMessageRequest = new SendMessageRequest(scoringQueueUrl, transaction);
            context.getLogger().log("ID --->> "+ id);
            amazonSQS.sendMessage(sendMessageRequest);
        });
        context.getLogger().log("Delivered "+ countOfRecords + " transaction events to SQS!");
        context.getLogger().log("Finished the event batch!");

        return null;
    }
}
