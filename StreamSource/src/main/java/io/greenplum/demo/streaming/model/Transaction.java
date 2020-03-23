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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;

/**
 * @author Sridhar Paladugu
 * @version 1.0
 */

public class Transaction implements Serializable {
    private static final long serialVersionUID = 8780959226199358281L;

    @JsonProperty("transaction_id")
    int transactionId;
    @JsonProperty("rlb_location_key")
    private int rlbLocationKey;
    @JsonProperty("location_id")
    private int locationId;
    @JsonProperty("account_id")
    private Integer accountId;
    @JsonProperty("account_number")
    private String accountNumber;
    @JsonProperty("account_state")
    private String accountState;
    @JsonProperty("card_type")
    private String cardType;
    @JsonProperty("merchant_city")
    private String merchantCity;
    @JsonProperty("merchant_city_alias")
    private String merchantCityAlias;
    @JsonProperty("merchant_name")
    private String merchantName;
    @JsonProperty("merchant_state")
    private String merchantState;
    @JsonProperty("merchant_long")
    private Double merchantLongitude;
    @JsonProperty("merchant_lat")
    private Double merchantLatitude;
    @JsonProperty("posting_date")
    private Long postingDate;
    @JsonProperty("sic_code")
    private String sicCode;
    @JsonProperty("transaction_amount")
    private Double transactionAmount;
    @JsonProperty("transaction_date")
    private Long transactionDate;
    @JsonProperty("account_longitude")
    private Double accountLongitude;
    @JsonProperty("account_latitude")
    private Double accountLatitude;
    @JsonProperty("fraud_flag")
    private Boolean fraudFlag;

    public Integer getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(Integer transactionId) {
        this.transactionId = transactionId;
    }

    public int getRlbLocationKey() {
        return rlbLocationKey;
    }

    public void setRlbLocationKey(int rlbLocationKey) {
        this.rlbLocationKey = rlbLocationKey;
    }

    public int getLocationId() {
        return locationId;
    }

    public void setLocationId(int locationId) {
        this.locationId = locationId;
    }

    public Integer getAccountId() {
        return accountId;
    }

    public void setAccountId(Integer accountId) {
        this.accountId = accountId;
    }

    public String getAccountNumber() {
        return accountNumber;
    }

    public void setAccountNumber(String accountNumber) {
        this.accountNumber = accountNumber;
    }

    public String getAccountState() {
        return accountState;
    }

    public void setAccountState(String accountState) {
        this.accountState = accountState;
    }

    public String getCardType() {
        return cardType;
    }

    public void setCardType(String cardType) {
        this.cardType = cardType;
    }

    public String getMerchantCity() {
        return merchantCity;
    }

    public void setMerchantCity(String merchantCity) {
        this.merchantCity = merchantCity;
    }

    public String getMerchantCityAlias() {
        return merchantCityAlias;
    }

    public void setMerchantCityAlias(String merchantCityAlias) {
        this.merchantCityAlias = merchantCityAlias;
    }

    public String getMerchantName() {
        return merchantName;
    }

    public void setMerchantName(String merchantName) {
        this.merchantName = merchantName;
    }

    public String getMerchantState() {
        return merchantState;
    }

    public void setMerchantState(String merchantState) {
        this.merchantState = merchantState;
    }

    public Double getMerchantLongitude() {
        return merchantLongitude;
    }

    public void setMerchantLongitude(Double merchantLongitude) {
        this.merchantLongitude = merchantLongitude;
    }

    public Double getMerchantLatitude() {
        return merchantLatitude;
    }

    public void setMerchantLatitude(Double merchantLatitude) {
        this.merchantLatitude = merchantLatitude;
    }

    public Long getPostingDate() {
        return postingDate;
    }

    public void setPostingDate(Long postingDate) {
        this.postingDate = postingDate;
    }

    public String getSicCode() {
        return sicCode;
    }

    public void setSicCode(String sicCode) {
        this.sicCode = sicCode;
    }

    public Double getTransactionAmount() {
        return transactionAmount;
    }

    public void setTransactionAmount(Double transactionAmount) {
        this.transactionAmount = transactionAmount;
    }

    public Long getTransactionDate() {
        return transactionDate;
    }

    public void setTransactionDate(Long transactionDate) {
        this.transactionDate = transactionDate;
    }

    public Double getAccountLongitude() {
        return accountLongitude;
    }

    public void setAccountLongitude(Double accountLongitude) {
        this.accountLongitude = accountLongitude;
    }

    public Double getAccountLatitude() {
        return accountLatitude;
    }

    public void setAccountLatitude(Double accountLatitude) {
        this.accountLatitude = accountLatitude;
    }

    public Boolean getFraudFlag() {
        return fraudFlag;
    }

    public void setFraudFlag(Boolean fraudFlag) {
        this.fraudFlag = fraudFlag;
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

    public static Transaction fromJsonAsBytes(byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, Transaction.class);
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

    public static Transaction fromJsonAsString(String json) {
        try {
            return objectMapper.readValue(json, Transaction.class);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "transactionId='" + transactionId + '\'' +
                ", rlbLocationKey=" + rlbLocationKey +
                ", locationId=" + locationId +
                ", accountId=" + accountId +
                ", accountNumber='" + accountNumber + '\'' +
                ", accountState='" + accountState + '\'' +
                ", cardType='" + cardType + '\'' +
                ", merchantCity='" + merchantCity + '\'' +
                ", merchantCityAlias='" + merchantCityAlias + '\'' +
                ", merchantName='" + merchantName + '\'' +
                ", merchantState='" + merchantState + '\'' +
                ", merchantLongitude=" + merchantLongitude +
                ", merchantLatitude=" + merchantLatitude +
                ", postingDate='" + postingDate + '\'' +
                ", sicCode='" + sicCode + '\'' +
                ", transactionAmount=" + transactionAmount +
                ", transactionDate='" + transactionDate + '\'' +
                ", accountLongitude=" + accountLongitude +
                ", accountLatitude=" + accountLatitude +
                ", fraudFlag=" + fraudFlag +
                '}';
    }


    /**
     * Produce delimited string of all properties in the below order
     * "account_id","account_lat","account_long","account_number","card_type","fraud_flag","location_id",
     * "merchant_city","merchant_city_alias","merchant_lat","merchant_long","merchant_name","merchant_state",
     * "posting_date","rlb_location_key","transaction_amount","transaction_date","transaction_id"
     *
     * @param delimiter
     * @return
     */
    @JsonIgnore
    public String toCSV(String delimiter) {
        if (delimiter == null || delimiter.trim().length() == 0) {
            delimiter = ",";
        }
        StringBuffer sb = new StringBuffer();
        sb.append(getAccountId()).append(delimiter);
        sb.append(getAccountLatitude()).append(delimiter);
        sb.append(getAccountLongitude()).append(delimiter);
        sb.append(getAccountNumber()).append(delimiter);
        sb.append("\"").append(getCardType()).append("\"").append(delimiter);
        sb.append(getFraudFlag()).append(delimiter);
        sb.append(getLocationId()).append(delimiter);
        sb.append("\"").append(getMerchantCity()).append("\"").append(delimiter);
        sb.append("\"").append(getMerchantCityAlias()).append("\"").append(delimiter);
        sb.append(getMerchantLatitude()).append(delimiter);
        sb.append(getMerchantLongitude()).append(delimiter);
        sb.append("\"").append(getMerchantName()).append("\"").append(delimiter);
        sb.append("\"").append(getMerchantState()).append("\"").append(delimiter);
        sb.append(getPostingDate()).append(delimiter);
        sb.append(getRlbLocationKey()).append(delimiter);
        sb.append(getTransactionAmount()).append(delimiter);
        sb.append(getTransactionDate()).append(delimiter);
        sb.append(getTransactionId());
        sb.append("\n");
        return sb.toString();
    }

    /**
     * Get header row for CSV
     * @return String
     */
    @JsonIgnore
    public String getCSVHeader() {
        String header = "\"account_id\",\"account_lat\",\"account_long\",\"account_number\",\"card_type\",\"fraud_flag\"," +
                "\"location_id\",\"merchant_city\",\"merchant_city_alias\",\"merchant_lat\",\"merchant_long\"," +
                "\"merchant_name\",\"merchant_state\",\"posting_date\",\"rlb_location_key\",\"transaction_amount\"," +
                "\"transaction_date\",\"transaction_id\"" + "\n";
        return header;
    }
    @JsonIgnore
    public String getPostingDateStr() {
        SimpleDateFormat dtFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String postingDateStr = dtFmt.format(getPostingDate());
        return postingDateStr;
    }
    @JsonIgnore
    public String getTransactionDateStr() {
        SimpleDateFormat dtFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String transactionDateStr = dtFmt.format(getTransactionDate());
        return transactionDateStr;
    }
}