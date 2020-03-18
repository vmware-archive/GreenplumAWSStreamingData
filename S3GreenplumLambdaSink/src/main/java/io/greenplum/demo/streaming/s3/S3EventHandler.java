package io.greenplum.demo.streaming.s3;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.event.S3EventNotification;
/**
 * @author Sridhar Paladugu
 * @version 1.0
 */
public class S3EventHandler implements RequestHandler<S3Event, Void> {

    @Override
    public Void handleRequest(S3Event s3Event, Context context) {
        S3EventNotification.S3EventNotificationRecord eventRec = s3Event.getRecords().get(0);
        String bucketName = eventRec.getS3().getBucket().getName();
        String objectKey = eventRec.getS3().getObject().getKey();
        StringBuffer sb= new StringBuffer();
        sb.append("EventName -> ").append( eventRec.getEventName()).append("\n");
        sb.append("EventSource -> ").append( eventRec.getEventSource()).append("\n");
        sb.append("EventTime -> ").append( eventRec.getEventTime()).append("\n");
        sb.append("EventRegion -> ").append( eventRec.getAwsRegion()).append("\n");
        sb.append("BucketName -> ").append(bucketName).append("\n");
        sb.append("objectKey -> ").append(objectKey).append("\n");
        context.getLogger().log(sb.toString());
        if(objectKey.endsWith("/")) {
            context.getLogger().log("Folder Event raised and this will be ignored!");
        } else {
            GreenplumWriter greenplumWriter = new GreenplumWriter();
            greenplumWriter.writeBatch(objectKey, context);
        }
        return null;
    }


}
