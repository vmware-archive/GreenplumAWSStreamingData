package io.greenplum.demo.streaming.sqs;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import io.greenplum.demo.streaming.sqs.model.ScoredCreditTransaction;

import java.nio.ByteBuffer;


/**
 * @author Sridhar Paladugu
 * @version 1.0
 *
 * This class implement the Event handler for SQS queue which harbor the scored
 * credit transactions. They will be delivered to a S3Firehose delivery stream.
 */
public class ScoredCreditTransactionsHandler implements RequestHandler<SQSEvent, Void> {
    private String region;
    private String deliveryStreamName ;
    private AmazonKinesisFirehose amazonKinesisFirehose;


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
    }

    @Override
    public Void handleRequest(SQSEvent sqsEvent, Context context) {
        if(amazonKinesisFirehose == null) {
            init(context);
        }
        StringBuffer csv = new StringBuffer("");
        int countOfRecords = sqsEvent.getRecords().size();
        for(SQSEvent.SQSMessage sqsMessage: sqsEvent.getRecords())  {
            String scoredTransactionJSON = sqsMessage.getBody();
            ScoredCreditTransaction scoredTransaction = ScoredCreditTransaction.fromJsonAsBytes(scoredTransactionJSON.getBytes());
            if (scoredTransaction == null) {
                context.getLogger().log("Scored Transaction received as null, so skipping record for partition -> ");
            } else {
                // to s3 as csv file with all transactions in the batch
                csv.append(scoredTransaction.toCSV(","));
            }
        }

        context.getLogger().log("Storing " + countOfRecords + " events to kinesis firehose ....");
        Record record = new Record().withData(ByteBuffer.wrap(csv.toString().getBytes()));
        PutRecordRequest putRecordRequest  = new PutRecordRequest()
                .withDeliveryStreamName(deliveryStreamName).withRecord(record);
        amazonKinesisFirehose.putRecord(putRecordRequest);
        context.getLogger().log("Delivered scored transaction events to S3!");

        return null;
    }
}
