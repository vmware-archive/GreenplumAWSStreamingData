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

import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import io.greenplum.demo.streaming.model.Transaction;
import io.greenplum.demo.streaming.sink.ConsoleSink;
import io.greenplum.demo.streaming.sink.KinesisStreamSink;
import io.greenplum.demo.streaming.source.TransactionSimulator;
import io.greenplum.demo.streaming.source.config.TransactionsSourceProperties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.List;
/**
 * @author Sridhar Paladugu
 * @version 1.0
 */
@SpringBootApplication
@EnableScheduling
public class CreditTransactionGeneratorApplication {
    @Autowired
    StreamStatefulRunner streamStatefulRunner;
    private static final Log log = LogFactory.getLog(CreditTransactionGeneratorApplication.class.getName());

    public static void main(String[] args) {
        SpringApplication.run(CreditTransactionGeneratorApplication.class, args);
    }

    @Scheduled(fixedDelay = 6000)
    public void run() {
        streamStatefulRunner.dispatch();
    }
}

