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

package io.greenplum.demo.streaming.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.greenplum.demo.streaming.model.Bounds;
import io.greenplum.demo.streaming.model.Transaction;
import io.greenplum.demo.streaming.source.TransactionSimulator;

import java.util.List;
/**
 * @author Sridhar Paladugu
 * @version 1.0
 */
public class StreamUtil {

    public static String convertToJson(List<Transaction> transactions ) {
        String json = null;
        if( null != transactions || transactions.size() > 0) {
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(transactions);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        return json;
    }

    /**
     * get lower and higher bounds based on the numerical precedence.
     * @param trxMeanValue
     * @param trxStdValue
     * @return
     */
    public static Bounds getBounds(Double trxMeanValue, Double trxStdValue) {
        Bounds bounds = new Bounds();
        if (trxMeanValue > trxStdValue) {
            bounds.setHigh(trxMeanValue);
            bounds.setLow(trxStdValue);
        }else {
            bounds.setHigh(trxStdValue);
            bounds.setLow(trxMeanValue);
        }
        return bounds;
    }
}
