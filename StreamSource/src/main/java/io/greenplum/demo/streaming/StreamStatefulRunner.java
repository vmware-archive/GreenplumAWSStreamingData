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

package io.greenplum.demo.streaming;


import com.amazonaws.services.kinesis.model.PutRecordsResult;
import io.greenplum.demo.streaming.model.Transaction;
import io.greenplum.demo.streaming.sink.ConsoleSink;
import io.greenplum.demo.streaming.sink.KinesisStreamSink;
import io.greenplum.demo.streaming.source.TransactionSimulator;
import io.greenplum.demo.streaming.source.config.TransactionsSourceProperties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Sridhar Paladugu
 * @version 1.0
 */

@Component
public class StreamStatefulRunner {
    @Autowired
    private TransactionSimulator transactionSimulator;
    @Autowired
    private TransactionsSourceProperties appProps;
    @Autowired
    private ConsoleSink consoleSink;

    @Autowired
    private KinesisStreamSink kinesisStreamSink;
    List<Transaction> retryTransactions = new ArrayList<Transaction>();

    private static final Log log = LogFactory.getLog(StreamStatefulRunner.class.getName());

    public void dispatch() {
        log.debug("------------------ Start new batch ------------------ ");
        List<Transaction> transactions = transactionSimulator.generateTransactions();
        if ("console".equals(appProps.getTarget())) {
            consoleSink.sink(transactions);
        }else if ("kinesis".equals(appProps.getTarget())) {
            if (kinesisStreamSink.getKinesisClient() == null ) {
                kinesisStreamSink.init();
            }
            log.info("Sending "+transactions.size()+ " records to Kinesis Stream...........");
            if("batch".equalsIgnoreCase(appProps.getDispatchStyle())){
                 kinesisStreamSink.dispathTransactions(transactions);
            } else if("single".equalsIgnoreCase(appProps.getDispatchStyle())) {
                transactions.forEach(transaction -> {
                    kinesisStreamSink.dispatchTransaction(transaction);
                });
            }
        }else {
            throw new UnsupportedOperationException("Target for payload is not understood. It should be Kinesis or Console!");
        }
        log.debug("------------------ End Batch ------------------ ");
    }
}
