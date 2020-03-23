package io.greenplum.demo.streaming.sqs;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.greenplum.demo.streaming.model.CreditTransaction;
import io.greenplum.demo.streaming.model.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author sridhar paladugu
 * @version 1.0
 *
 * The class defines event handler to sink data to S3 firehore from a Kenisis Event.
 *
 */
@Component
public class SQSTransactionScoreEventHandler implements RequestHandler<SQSEvent, Void> {

    @Autowired
    RTSMadlibService rtsMadlibService;
    private AmazonSQS amazonSQS;
    public SQSTransactionScoreEventHandler() {
        amazonSQS  = AmazonSQSClientBuilder.defaultClient();
        rtsMadlibService = new RTSMadlibService();
    }

    private String scoredQueueUrl;
    @Override
    public Void handleRequest(SQSEvent sqsEvent, Context context) {
        for(SQSEvent.SQSMessage sqsMessage: sqsEvent.getRecords())  {
            String transactionJSON = sqsMessage.getBody();
            Transaction transaction = Transaction.fromJsonAsBytes(transactionJSON.getBytes());
            Integer id = transaction.getTransactionId();
            CreditTransaction ct = createPayLoad(transaction);
            String scoredTransaction = scoreTransaction(ct.toJsonAsString(), context);
            dispatchScoredTransaction(id.toString(), scoredTransaction);
        }
        return null;
    }

    private String scoreTransaction(String payload, Context context) {
        context.getLogger().log("<<<< ML_PAYLOAD >>>> " + payload);
        List<Map<String, Object>> l = rtsMadlibService.process(payload, context);
        Map<String, Object> m = l.get(0);
        String scoredJson = null;
        ObjectMapper om = new ObjectMapper();
        try {
            scoredJson = om.writeValueAsString(m);
            context.getLogger().log("<<<< ML_SCORE_RESULTS >>>> " + scoredJson);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return scoredJson;
    }

    private void dispatchScoredTransaction(String id, String transaction) {
        if(null == scoredQueueUrl) {
            scoredQueueUrl = System.getenv("SCORED_TRANSACTION_QUEUE_URL");
        }
        SendMessageRequest sendMessageRequest = new SendMessageRequest(scoredQueueUrl, transaction);
        amazonSQS.sendMessage(sendMessageRequest);
    }
    private String formattedDate(Long dtLong) {
        if (null == dtLong ) return "";
        String fmtStr = "yyyy-MM-dd hh:mm:ss";
        SimpleDateFormat dateFormater = new SimpleDateFormat(fmtStr);
        String dt = dateFormater.format(new Date(dtLong));
        return dt;
    }
    private CreditTransaction createPayLoad(Transaction t) {
        CreditTransaction ct = new CreditTransaction(t.getAccountId(),
                t.getAccountNumber(),
                t.getCardType(),
                t.getFraudFlag(),
                t.getLocationId(),
                t.getMerchantCity(),
                t.getMerchantCityAlias(),
                t.getMerchantLatitude(),
                t.getMerchantLongitude(),
                t.getMerchantName(),
                t.getMerchantState(),
                formattedDate(t.getPostingDate()),
                t.getRlbLocationKey()+"",
                t.getTransactionAmount(),
                formattedDate(t.getTransactionDate()),
                t.getTransactionId());
        return ct;
    }

}
