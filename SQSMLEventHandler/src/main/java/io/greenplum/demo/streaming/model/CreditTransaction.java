package io.greenplum.demo.streaming.model;
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;

/**
 * @author Sridhar Paladugu
 * @version 1.0
 */

public class CreditTransaction implements Serializable {
    private static final long serialVersionUID = 9028122426885741704L;
    private Integer account_id;
    private String account_number;
    private String card_type;
    private Boolean fraud_flag;
    private Integer location_id;
    private String merchant_city;
    private String merchant_city_alias;
    private Double merchant_lat;
    private Double merchant_long;
    private String merchant_name;
    private String merchant_state;
    private String posting_date; //"yyyy-MM-dd hh:mm:ss",
    private String rlb_location_key;
    private Double transaction_amount;
    private String transaction_date; //"yyyy-MM-dd hh:mm:ss",
    private Integer transaction_id;

    public CreditTransaction() {
    }

    public CreditTransaction(Integer account_id, String account_number,
                             String card_type, Boolean fraud_flag, Integer location_id, String merchant_city,
                             String merchant_city_alias, Double merchant_lat, Double merchant_long,
                             String merchant_name, String merchant_state, String posting_date, String rlb_location_key,
                             Double transaction_amount, String transaction_date, Integer transaction_id) {
        this.account_id = account_id;
        this.account_number = account_number;

        this.card_type = card_type;
        this.fraud_flag = fraud_flag;
        this.location_id = location_id;
        this.merchant_city = merchant_city;
        this.merchant_city_alias = merchant_city_alias;
        this.merchant_lat = merchant_lat;
        this.merchant_long = merchant_long;
        this.merchant_name = merchant_name;
        this.merchant_state = merchant_state;
        this.posting_date = posting_date;
        this.rlb_location_key = rlb_location_key;
        this.transaction_amount = transaction_amount;
        this.transaction_date = transaction_date;
        this.transaction_id = transaction_id;
    }

    public Integer getAccount_id() {
        return account_id;
    }

    public void setAccount_id(Integer account_id) {
        this.account_id = account_id;
    }

    public String getAccount_number() {
        return account_number;
    }

    public void setAccount_number(String account_number) {
        this.account_number = account_number;
    }

    public String getCard_type() {
        return card_type;
    }

    public void setCard_type(String card_type) {
        this.card_type = card_type;
    }

    public Boolean getFraud_flag() {
        return fraud_flag;
    }

    public void setFraud_flag(Boolean fraud_flag) {
        this.fraud_flag = fraud_flag;
    }

    public Integer getLocation_id() {
        return location_id;
    }

    public void setLocation_id(Integer location_id) {
        this.location_id = location_id;
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

    public Double getMerchant_lat() {
        return merchant_lat;
    }

    public void setMerchant_lat(Double merchant_lat) {
        this.merchant_lat = merchant_lat;
    }

    public Double getMerchant_long() {
        return merchant_long;
    }

    public void setMerchant_long(Double merchant_long) {
        this.merchant_long = merchant_long;
    }

    public String getMerchant_name() {
        return merchant_name;
    }

    public void setMerchant_name(String merchant_name) {
        this.merchant_name = merchant_name;
    }

    public String getMerchant_state() {
        return merchant_state;
    }

    public void setMerchant_state(String merchant_state) {
        this.merchant_state = merchant_state;
    }

    public String getPosting_date() {
        return posting_date;
    }

    public void setPosting_date(String posting_date) {
        this.posting_date = posting_date;
    }

    public String getRlb_location_key() {
        return rlb_location_key;
    }

    public void setRlb_location_key(String rlb_location_key) {
        this.rlb_location_key = rlb_location_key;
    }

    public Double getTransaction_amount() {
        return transaction_amount;
    }

    public void setTransaction_amount(Double transaction_amount) {
        this.transaction_amount = transaction_amount;
    }

    public String getTransaction_date() {
        return transaction_date;
    }

    public void setTransaction_date(String transaction_date) {
        this.transaction_date = transaction_date;
    }

    public Integer getTransaction_id() {
        return transaction_id;
    }

    public void setTransaction_id(Integer transaction_id) {
        this.transaction_id = transaction_id;
    }

    @Override
    public String toString() {
        return "CreditTransaction{" +
                "account_id=" + account_id +
                ", account_number='" + account_number + '\'' +
                ", card_type='" + card_type + '\'' +
                ", fraud_flag=" + fraud_flag +
                ", location_id=" + location_id +
                ", merchant_city='" + merchant_city + '\'' +
                ", merchant_city_alias='" + merchant_city_alias + '\'' +
                ", merchant_lat=" + merchant_lat +
                ", merchant_long=" + merchant_long +
                ", merchant_name='" + merchant_name + '\'' +
                ", merchant_state='" + merchant_state + '\'' +
                ", posting_date='" + posting_date + '\'' +
                ", rlb_location_key='" + rlb_location_key + '\'' +
                ", transaction_amount=" + transaction_amount +
                ", transaction_date='" + transaction_date + '\'' +
                ", transaction_id=" + transaction_id +
                '}';
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

    public static CreditTransaction fromJsonAsBytes(byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, CreditTransaction.class);
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

    public static CreditTransaction fromJsonAsString(String json) {
        try {
            return objectMapper.readValue(json, CreditTransaction.class);
        } catch (IOException e) {
            return null;
        }
    }
}
