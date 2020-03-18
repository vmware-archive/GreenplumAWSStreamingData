package io.greenplum.demo.streaming.firehose;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesisfirehose.model.*;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.greenplum.demo.streaming.model.Transaction;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author sridhar paladugu
 * @version 1.0
 *
 * The class defines event handler to sink data to S3 firehore from a Kenisis Event.
 *
 */
public class CreditTransactionsFirehoseHandler implements RequestHandler<KinesisEvent , Void> {

    private String region;
    private String deliveryStreamName ;
    private AmazonKinesisFirehose amazonKinesisFirehose;

    static final Properties props = new Properties();
    static {
        try {
            Reader reader = new FileReader("app.properties");
            props.load(reader);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Cannot find app.properties", e);
        } catch (IOException e) {
            throw new RuntimeException("Error loading app.properties", e);
        }
    }

    public void init(Context context) {
        context.getLogger().log("Initializing Kinesis firehose.......");
        region = props.get("awsRegion").toString();
        deliveryStreamName = props.get("deliveryStreamName").toString();
        AmazonKinesisFirehoseClientBuilder amazonKinesisFirehoseClientBuilder = AmazonKinesisFirehoseClientBuilder.standard();
         ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setConnectionTimeout(60);
        amazonKinesisFirehoseClientBuilder.setClientConfiguration(clientConfiguration);
        amazonKinesisFirehoseClientBuilder.setRegion(region);
        amazonKinesisFirehose = amazonKinesisFirehoseClientBuilder.build();
        context.getLogger().log("Finished Initializing Kinesis firehose!");
    }


    @Override
    public Void handleRequest(KinesisEvent kinesisEvent, Context context) {
        if(amazonKinesisFirehose == null) {
            init(context);
        }
        int countOfRecords = kinesisEvent.getRecords().size();
        context.getLogger().log("Received "+ countOfRecords +" transactions in the current batch.");
        List<Transaction> transactionList = new ArrayList<Transaction>();
        StringBuffer csv = null;
        for (KinesisEvent.KinesisEventRecord rec : kinesisEvent.getRecords()) {
            Transaction transaction = Transaction.fromJsonAsBytes(rec.getKinesis().getData().array());
            if (transaction == null) {
                context.getLogger().log("Transaction received as null, so skipping record for partition -> " + rec.getKinesis().getPartitionKey());
            } else {
                if (csv == null) {
                    csv = new StringBuffer();
//                    csv.append(transaction.getCSVHeader());
                }
                csv.append(transaction.toCSV(","));
            }

        }
        context.getLogger().log("Storing " + countOfRecords + " events to s3 via firehose ....");
        Record record = new Record().withData(ByteBuffer.wrap(csv.toString().getBytes()));
        PutRecordRequest putRecordRequest  = new PutRecordRequest()
                .withDeliveryStreamName(deliveryStreamName).withRecord(record);
        amazonKinesisFirehose.putRecord(putRecordRequest);
        context.getLogger().log("Delivered transaction events to S3!");
        return null;
    }
}
