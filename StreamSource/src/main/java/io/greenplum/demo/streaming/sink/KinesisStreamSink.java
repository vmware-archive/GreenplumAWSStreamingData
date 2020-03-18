/**********************************************************************************************
 Copyright 2020 VMWare Inc
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 *********************************************************************************************/

package io.greenplum.demo.streaming.sink;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.*;
import io.greenplum.demo.streaming.model.Transaction;
import io.greenplum.demo.streaming.source.config.TransactionsSourceProperties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Sridhar Paladugu
 * @version 1.0
 */
@Component
public class KinesisStreamSink {

    @Autowired
    TransactionsSourceProperties props;
    private static final Log log = LogFactory.getLog(KinesisStreamSink.class.getName());

    public AmazonKinesis getKinesisClient() {
        return kinesisClient;
    }

    private AmazonKinesis kinesisClient = null;

    /**
     * Initialize Kinesis client and establish a session
     */
    public void init() {
        try {
            if (kinesisClient != null) return;
            checkRegion();
            AmazonKinesisClientBuilder amazonKinesisClientBuilder = AmazonKinesisClientBuilder.standard();
            amazonKinesisClientBuilder.setRegion(props.getRegionName());
            BasicAWSCredentials credentials = new BasicAWSCredentials(props.getAwsAccessKey(), props.getAwsSecretKey());
            AWSCredentialsProvider awsCredentialsProvider = new AWSStaticCredentialsProvider(credentials);
            amazonKinesisClientBuilder.setCredentials(awsCredentialsProvider);
            ClientConfiguration clientConfiguration = new ClientConfiguration();
            clientConfiguration.setConnectionTimeout(60);
            amazonKinesisClientBuilder.setClientConfiguration(clientConfiguration);
            kinesisClient = amazonKinesisClientBuilder.build();
            checkStream();
        } catch (Throwable t) {
            throw new RuntimeException("ERROR: Initializing AWS Kinesis client.", t);
        }
    }

    /**
     * Verify the region exists.
     *
     * @throws RuntimeException if no region exists with the specified name
     */
    private void checkRegion() {
        String regionName = props.getRegionName();
        Region region = RegionUtils.getRegion(regionName);
        if (region == null) {
            log.error(regionName + " is not a valid AWS region. Quitting.");
            throw new RuntimeException(regionName + " is not a valid AWS region.");
        }
        String streamName = props.getStreamName();

    }

    /**
     * Verify the Kinesis Stream exists.
     *
     * @throws RuntimeException if no stream exists with the specified name
     */
    private void checkStream() {
        String streamName = props.getStreamName();
        try {
            DescribeStreamResult result = kinesisClient.describeStream(streamName);
            String status = result.getStreamDescription().getStreamStatus();
            if (!"ACTIVE".equals(status)) {
                String msg = "Stream " + streamName + " status is invalid:  " + status;
                throw new RuntimeException(msg);
            }
        } catch (ResourceNotFoundException e) {
            String msg = "Stream " + streamName + " not found. Please create Stream in Kinesis.";
            throw new RuntimeException(msg);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * writes a Single Transaction record to Kinesis Stream
     *
     * @param transaction
     * @throws RuntimeException if any error in the process.
     */
    public void dispatchTransaction(Transaction transaction) {
        if (null == transaction) {
            throw new RuntimeException("Transaction cannot be null, exiting!");
        }
        byte[] bytes = transaction.toJsonAsBytes();
        log.debug("dispatching  transaction to stream : " + transaction.toString());
        PutRecordRequest putRecord = new PutRecordRequest();
        putRecord.setStreamName(props.getStreamName());
        putRecord.setPartitionKey(transaction.getAccountState());
        putRecord.setData(ByteBuffer.wrap(bytes));
        try {
            kinesisClient.putRecord(putRecord);
        } catch (AmazonClientException ex) {
            throw new RuntimeException("Error dispatching transaction with id ->" + transaction.getTransactionId(), ex);
        }
    }

    /**
     * write a batch of events to kinesis stream
     *
     * @param transactions
     * @return link {@PutRecordsResult}
     */
    public PutRecordsResult dispathTransactions(List<Transaction> transactions) {
        if (null == transactions) {
            throw new RuntimeException("Transactions list cannot be null, exiting!");
        } else if (transactions.size() == 0) {
            throw new RuntimeException("No Transactions received, quitting!");
        } else {
            PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
            putRecordsRequest.setStreamName(props.getStreamName());
            List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<PutRecordsRequestEntry>();
            transactions.forEach(transaction -> {
                PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
                putRecordsRequestEntry.setData(ByteBuffer.wrap(transaction.toJsonAsBytes()));
                putRecordsRequestEntry.setPartitionKey(String.format("partitionKey-%s", transaction.getAccountState()));
                putRecordsRequestEntryList.add(putRecordsRequestEntry);
            });
            putRecordsRequest.setRecords(putRecordsRequestEntryList);
            PutRecordsResult putRecordsResult = kinesisClient.putRecords(putRecordsRequest);

            if (putRecordsResult.getFailedRecordCount() != 0) {
                log.warn("There are " + putRecordsResult.getFailedRecordCount()
                        + " records failed to dispatch." +
                        " I will retry them one time.");

                List<PutRecordsResultEntry> putRecordsResultEntryList = putRecordsResult.getRecords();
                final List<PutRecordsRequestEntry> failedRecordsList = new ArrayList<>();
                for (int i = 0; i < putRecordsResultEntryList.size(); i++) {
                    final PutRecordsRequestEntry putRecordRequestEntry = putRecordsRequestEntryList.get(i);
                    final PutRecordsResultEntry putRecordsResultEntry = putRecordsResultEntryList.get(i);
                    if (putRecordsResultEntry.getErrorCode() != null) {
                        failedRecordsList.add(putRecordRequestEntry);
                    }
                }
                putRecordsRequest = new PutRecordsRequest();
                putRecordsRequest.setRecords(failedRecordsList);
                putRecordsResult = kinesisClient.putRecords(putRecordsRequest);
            }
            return putRecordsResult;
        }
    }

}

