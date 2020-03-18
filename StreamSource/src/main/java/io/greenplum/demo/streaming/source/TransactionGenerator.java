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

package io.greenplum.demo.streaming.source;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.greenplum.demo.streaming.model.Account;
import io.greenplum.demo.streaming.model.Transaction;
import io.greenplum.demo.streaming.model.Location;
import io.greenplum.demo.streaming.source.config.TransactionsSourceProperties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author Sridhar Paladugu
 * @version 1.0
 *
 */
@Deprecated
@Component
public class TransactionGenerator {
	private static final Log log = LogFactory.getLog(TransactionGenerator.class.getName());
	@Autowired
	private TransactionsSourceProperties appProperties;
	private List<Location> locations;
	private List<Account> accounts;
	@Autowired
	private ResourceLoader resourceLoader;
	
	public String generate() {
		log.info("\n\t\t\t <================== START GENERATING TRANSACTIONS ==================>");
		if (null == this.locations) {
			this.locations = new ArrayList<Location>();
			this.locations = loadLocations();
		}
		if (null == this.accounts) {
			this.accounts = new ArrayList<Account>();
			this.accounts = loadAccounts();
		}
		int count = 0;
		Set<Transaction> transactions = new HashSet<Transaction>();
		while (count < this.appProperties.getTransactionsNumber()) {
			transactions.add(generate_transaction());
			count++;
		}
		ObjectMapper mapper = new ObjectMapper();
		String jsonData;
		try {
			jsonData = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(transactions);
			log.info("\n\t\t\t <================== FINISHED GENERATING TRANSACTIONS ==================>");
			return jsonData;
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}

	}

	private Transaction generate_transaction() {
		SimpleDateFormat dtFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Location location = (Location) this.locations.get(new Random().nextInt(this.locations.size()));
		Account account = (Account) this.accounts.get(new Random().nextInt(this.accounts.size()));
		Double trxnAmount = Double.valueOf(account.getTrxnMean().doubleValue() + new Random().nextGaussian() * account.getTrxStd().doubleValue());
//		String postingDate = dtFmt.format(Calendar.getInstance().getTime());
		Long postingDate = Calendar.getInstance().getTime().getTime();
		Date dt = Calendar.getInstance().getTime();
//		String transactionDate = dtFmt.format(dt);
		Long transactionDate = dt.getTime();
		Transaction transaction = new Transaction();
		StringBuffer id = new StringBuffer();
		id.append(UUID.randomUUID().toString())
		  .append("-")
		  .append(account.getAccountNumber())
		  .append("-"+dt.getTime()) 
		  ;
		transaction.setTransactionId(id.toString());
		transaction.setRlbLocationKey(location.getRlbLocationKey());
		transaction.setAccountNumber(account.getAccountNumber());
		transaction.setCardType(account.getCardType());
		transaction.setMerchantCity(location.getMerchantCity());
		transaction.setMerchantName(location.getMerchantName());
		transaction.setMerchantState(location.getMerchantState());
		transaction.setPostingDate(postingDate);
//		transaction.setSicCode(location.getSicCode());
		transaction.setTransactionAmount(trxnAmount);
		transaction.setTransactionDate(transactionDate);

		return transaction;
	}

	private List<Location> loadLocations() {
		ObjectMapper mapper = new ObjectMapper();
		TypeReference<List<Location>> locationMapType = new TypeReference<List<Location>>() {
		};
		try {
			Resource resource = resourceLoader.getResource("classpath:locations.json");
	        InputStream is = resource.getInputStream();
			return mapper.readValue(is, locationMapType);
		} catch (Exception e) {
			throw new RuntimeException("Error loading locations JSON file.", e);
		}
	}

	private List<Account> loadAccounts() {
		ObjectMapper mapper = new ObjectMapper();
		TypeReference<List<Account>> acctMapType = new TypeReference<List<Account>>() {
		};
		try {
			Resource resource = resourceLoader.getResource("classpath:accounts.json");
	        InputStream is = resource.getInputStream();
			this.accounts = mapper.readValue(is, acctMapType);
			return this.accounts;
		} catch (Exception e) {
			throw new RuntimeException("Error loading accounts JSON file.", e);
		}
	}
}

