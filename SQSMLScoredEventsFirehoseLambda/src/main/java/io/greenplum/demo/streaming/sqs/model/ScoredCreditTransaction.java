package io.greenplum.demo.streaming.sqs.model;
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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;

/**
 * @author Sridhar Paladugu
 * @version 1.0
 */

public class ScoredCreditTransaction implements Serializable {

    private Integer transaction_id;
    private String transaction_date; //"yyyy-MM-dd hh:mm:ss",
    private String account_number;
    private String merchant_city;
    private String merchant_city_alias;
    private Double transaction_amount;
    private String merchant_name;
    private String card_type;
    private Double merchant_lat;
    private Integer location_id;
    private Double estimated_prob_true;
    private Boolean fraud_flag;
    private Integer account_id;
    private String merchant_state;
    private Integer merchant_state_2;
    private Double estimated_prob_false;
    private Double a_transaction_delta;
    private Double merchant_long;
    private String rlb_location_key;
    private Integer m_fraud_cases;
    private Double m_transaction_delta;
    private String posting_date; //"yyyy-MM-dd hh:mm:ss",

    /**
     * Get header row for CSV
     * @return String
     */
    @JsonIgnore
    public String getCSVHeader() {
        String header = "\"transaction_id\",\"transaction_date\",\"account_number\",\"merchant_city\","+
                "\"merchant_city_alias\",\"transaction_amount\",\"merchant_name\",\"card_type\",\"merchant_lat\"," +
                "\"location_id\",\"estimated_prob_true\",\"fraud_flag\",\"account_id\",\"merchant_state\",\"merchant_state_2\"" +
                ",\"estimated_prob_false\",\"a_transaction_delta\",\"merchant_long\",\"rlb_location_key\", \"m_fraud_cases\""+
                "\"m_transaction_delta\",\"posting_date\"\n";

        return header;
    }
    @JsonIgnore
    public String toCSV(String delimiter) {
        if (delimiter == null || delimiter.trim().length() == 0) {
            delimiter = ",";
        }
        StringBuffer sb = new StringBuffer();
        sb.append(getTransaction_id()).append(delimiter);
        sb.append("\"").append(getTransaction_date()).append("\"").append(delimiter);
        sb.append("\"").append(getAccount_number()).append("\"").append(delimiter);
        sb.append("\"").append(getMerchant_city()).append("\"").append(delimiter);
        sb.append("\"").append(getMerchant_city_alias()).append("\"").append(delimiter);
        sb.append(getTransaction_amount()).append(delimiter);
        sb.append("\"").append(getMerchant_name()).append("\"").append(delimiter);
        sb.append("\"").append(getCard_type()).append("\"").append(delimiter);
        sb.append(getMerchant_lat()).append(delimiter);
        sb.append(getLocation_id()).append(delimiter);
        sb.append(getEstimated_prob_true()).append(delimiter);
        sb.append(getFraud_flag()).append(delimiter);
        sb.append(getAccount_id()).append(delimiter);
        sb.append("\"").append(getMerchant_state()).append("\"").append(delimiter);
        sb.append(getMerchant_state_2()).append(delimiter);
        sb.append(getEstimated_prob_false()).append(delimiter);
        sb.append(getA_transaction_delta()).append(delimiter);
        sb.append(getMerchant_long()).append(delimiter);
        sb.append("\"").append(getRlb_location_key()).append("\"").append(delimiter);
        sb.append(getM_fraud_cases()).append(delimiter);
        sb.append(getM_transaction_delta()).append(delimiter);
        sb.append("\"").append(getPosting_date()).append("\"");
        sb.append("\n");
        return sb.toString();
    }

    private final static ObjectMapper objectMapper = new ObjectMapper();
    static {
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public byte[] toJsonAsBytes() {
        try {
            return objectMapper.writeValueAsBytes(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException((e));
        }
    }

    public static ScoredCreditTransaction fromJsonAsBytes(byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, ScoredCreditTransaction.class);
        } catch (IOException e) {
            return null;
        }
    }
    public String toJsonAsString() {
        try {
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException((e));
        }
    }

    public static ScoredCreditTransaction fromJsonAsString(String json) {
        try {
            return objectMapper.readValue(json, ScoredCreditTransaction.class);
        } catch (IOException e) {
            return null;
        }
    }


    public Integer getTransaction_id() {
        return transaction_id;
    }

    public void setTransaction_id(Integer transaction_id) {
        this.transaction_id = transaction_id;
    }

    public String getTransaction_date() {
        return transaction_date;
    }

    public void setTransaction_date(String transaction_date) {
        this.transaction_date = transaction_date;
    }

    public String getAccount_number() {
        return account_number;
    }

    public void setAccount_number(String account_number) {
        this.account_number = account_number;
    }

    public String getMerchant_city() {
        return merchant_city;
    }

    public void setMerchant_city(String merchant_city) {
        this.merchant_city = merchant_city;
    }

    public String getMerchant_city_alias() {
        return merchant_city_alias;
    }

    public void setMerchant_city_alias(String merchant_city_alias) {
        this.merchant_city_alias = merchant_city_alias;
    }

    public Double getTransaction_amount() {
        return transaction_amount;
    }

    public void setTransaction_amount(Double transaction_amount) {
        this.transaction_amount = transaction_amount;
    }

    public String getMerchant_name() {
        return merchant_name;
    }

    public void setMerchant_name(String merchant_name) {
        this.merchant_name = merchant_name;
    }

    public String getCard_type() {
        return card_type;
    }

    public void setCard_type(String card_type) {
        this.card_type = card_type;
    }

    public Double getMerchant_lat() {
        return merchant_lat;
    }

    public void setMerchant_lat(Double merchant_lat) {
        this.merchant_lat = merchant_lat;
    }

    public Integer getLocation_id() {
        return location_id;
    }

    public void setLocation_id(Integer location_id) {
        this.location_id = location_id;
    }

    public Double getEstimated_prob_true() {
        return estimated_prob_true;
    }

    public void setEstimated_prob_true(Double estimated_prob_true) {
        this.estimated_prob_true = estimated_prob_true;
    }

    public Boolean getFraud_flag() {
        return fraud_flag;
    }

    public void setFraud_flag(Boolean fraud_flag) {
        this.fraud_flag = fraud_flag;
    }

    public Integer getAccount_id() {
        return account_id;
    }

    public void setAccount_id(Integer account_id) {
        this.account_id = account_id;
    }

    public String getMerchant_state() {
        return merchant_state;
    }

    public void setMerchant_state(String merchant_state) {
        this.merchant_state = merchant_state;
    }

    public Integer getMerchant_state_2() {
        return merchant_state_2;
    }

    public void setMerchant_state_2(Integer merchant_state_2) {
        this.merchant_state_2 = merchant_state_2;
    }

    public Double getEstimated_prob_false() {
        return estimated_prob_false;
    }

    public void setEstimated_prob_false(Double estimated_prob_false) {
        this.estimated_prob_false = estimated_prob_false;
    }

    public Double getA_transaction_delta() {
        return a_transaction_delta;
    }

    public void setA_transaction_delta(Double a_transaction_delta) {
        this.a_transaction_delta = a_transaction_delta;
    }

    public Double getMerchant_long() {
        return merchant_long;
    }

    public void setMerchant_long(Double merchant_long) {
        this.merchant_long = merchant_long;
    }

    public String getRlb_location_key() {
        return rlb_location_key;
    }

    public void setRlb_location_key(String rlb_location_key) {
        this.rlb_location_key = rlb_location_key;
    }

    public Integer getM_fraud_cases() {
        return m_fraud_cases;
    }

    public void setM_fraud_cases(Integer m_fraud_cases) {
        this.m_fraud_cases = m_fraud_cases;
    }

    public Double getM_transaction_delta() {
        return m_transaction_delta;
    }

    public void setM_transaction_delta(Double m_transaction_delta) {
        this.m_transaction_delta = m_transaction_delta;
    }

    public String getPosting_date() {
        return posting_date;
    }

    public void setPosting_date(String posting_date) {
        this.posting_date = posting_date;
    }
}
